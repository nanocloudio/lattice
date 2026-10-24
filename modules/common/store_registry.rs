//! The store provider's subscription registry: one **watch** per distinct
//! prefix, each with the list of its **subscribers**, and an **index** —
//! the watches sorted by prefix — that finds every watch holding a key in
//! O(log W + depth), however many subscribers there are.
//!
//! A key is matched by binary-searching its predecessor in the index and
//! following parent links (a watch's parent is the longest other watch that
//! is a prefix of it). Every watch that is a prefix of the key lies on that
//! chain: a string between a prefix P of K and K itself starts with P, so P
//! is a prefix of the predecessor too. Prefixes need not end at `/`.
//!
//! Records live in fixed-size pages the caller allocates from the
//! provider's heap as they are needed, kept on per-kind free lists and never
//! returned: the registry costs what is subscribed, up to the heap the
//! module was packed with, and running out is the caller's counted refusal.
//!
//! Ids are `((page << 8) | slot) + 1`: a page index and a slot within it,
//! so a handle names a record without a table that has to be sized in
//! advance (the page table's length is a format bound, [`MAX_PAGES`]). Id 0
//! is [`NIL`], so a registry whose bytes are all zero is an empty one — the
//! provider that holds it is cleared in place, never constructed.
//!
//! No allocation of its own and no panic path.

/// Copy `src` into the front of `dst` (as much as fits). `copy_from_slice`
/// carries a panic path a PIC module cannot link.
fn copy(dst: &mut [u8], src: &[u8]) {
    for (d, s) in dst.iter_mut().zip(src) {
        *d = *s;
    }
}

/// Bytes of one page.
pub const PAGE: usize = 4096;
/// Pages the registry can address: the page part of an id is 12 bits.
pub const MAX_PAGES: usize = 4096;
/// Index pages a registry may use (each holds [`IDX_PER_PAGE`] watch ids).
pub const MAX_INDEX_PAGES: usize = 64;
pub const IDX_PER_PAGE: usize = PAGE / 4;
/// The longest prefix.
pub const PREFIX_MAX: usize = 255;
/// No record.
pub const NIL: u32 = 0;

/// One watch: a distinct prefix and its subscribers.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Watch {
    pub used: bool,
    pub plen: u8,
    _pad: [u8; 2],
    /// Free-list link while unused.
    link: u32,
    /// The longest other watch that is a prefix of this one, or [`NIL`].
    pub parent: u32,
    /// The first subscriber, or [`NIL`].
    pub subs: u32,
    pub prefix: [u8; PREFIX_MAX],
}

impl Watch {
    pub fn prefix(&self) -> &[u8] {
        &self.prefix[..self.plen as usize]
    }
}

/// One subscriber.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Sub {
    pub used: bool,
    /// Rung and not yet taken.
    pub rung: bool,
    /// A read it named was refused for now; it is rung again when the cause
    /// clears.
    pub deferred: bool,
    /// The length of the scope's head its prefix was named without (a
    /// scoped prefix's; 0 otherwise): a repeated SUBSCRIBE matches on it.
    pub strip: u8,
    /// Free-list link while unused; the next subscriber of the same watch
    /// while used.
    pub next: u32,
    /// The next deferred subscriber, or [`NIL`].
    pub defer_next: u32,
    pub watch: u32,
    /// The caller's tagged event fd.
    pub event: i32,
    /// The subscriber's owner record (`CALLER_OWNER`).
    pub owner: [u8; 8],
    /// The subscriber's module occupancy (`CALLER_MODULE`).
    pub module: [u8; 8],
    /// The provider step in which it holds first claim on the history-walk
    /// budget (it was refused the step before); 0 for none.
    pub prio: u32,
}

/// The fields a record kind shares with the slab that holds it.
trait Slot: Copy {
    const EMPTY: Self;
    fn used(&self) -> bool;
    fn link(&self) -> u32;
    fn free(&mut self, next: u32);
}

impl Slot for Watch {
    const EMPTY: Self = Watch {
        used: false,
        plen: 0,
        _pad: [0; 2],
        link: NIL,
        parent: NIL,
        subs: NIL,
        prefix: [0; PREFIX_MAX],
    };
    fn used(&self) -> bool {
        self.used
    }
    fn link(&self) -> u32 {
        self.link
    }
    fn free(&mut self, next: u32) {
        *self = Self::EMPTY;
        self.link = next;
    }
}

impl Slot for Sub {
    const EMPTY: Self = Sub {
        used: false,
        rung: false,
        deferred: false,
        strip: 0,
        next: NIL,
        defer_next: NIL,
        watch: NIL,
        event: -1,
        owner: [0; 8],
        module: [0; 8],
        prio: 0,
    };
    fn used(&self) -> bool {
        self.used
    }
    fn link(&self) -> u32 {
        self.next
    }
    fn free(&mut self, next: u32) {
        *self = Self::EMPTY;
        self.next = next;
    }
}

const fn per_page<T>() -> usize {
    let n = PAGE / core::mem::size_of::<T>();
    if n > 256 {
        256
    } else {
        n
    }
}

// Where a registry's pages come from: a visitor `alloc(PAGE)` returning a
// fresh, writable, 8-byte-aligned page that lives as long as the registry,
// or null when the heap is spent. Generic, never `dyn`: a vtable is a table
// of absolute pointers, which a PIC module cannot hold.

/// The page table every slab draws from.
struct Pages {
    table: [*mut u8; MAX_PAGES],
    n: usize,
}

impl Pages {
    const fn new() -> Self {
        Pages {
            table: [core::ptr::null_mut(); MAX_PAGES],
            n: 0,
        }
    }

    fn take<A: FnMut(usize) -> *mut u8>(&mut self, alloc: &mut A) -> Option<u16> {
        if self.n >= MAX_PAGES {
            return None;
        }
        let p = alloc(PAGE);
        if p.is_null() || !(p as usize).is_multiple_of(8) {
            return None;
        }
        self.table[self.n] = p;
        self.n += 1;
        Some((self.n - 1) as u16)
    }
}

/// Records of one kind, in pages, on a free list.
struct Slab<T: Slot> {
    free: u32,
    /// Page-table indices of this slab's pages, in order.
    pages: [u16; MAX_PAGES],
    n_pages: usize,
    live: u32,
    _t: core::marker::PhantomData<T>,
}

impl<T: Slot> Slab<T> {
    const fn new() -> Self {
        Slab {
            free: NIL,
            pages: [0; MAX_PAGES],
            n_pages: 0,
            live: 0,
            _t: core::marker::PhantomData,
        }
    }

    fn ptr(pages: &Pages, id: u32) -> Option<*mut T> {
        let raw = id.checked_sub(1)?;
        let page = (raw >> 8) as usize;
        let slot = (raw & 0xFF) as usize;
        if page >= pages.n || slot >= per_page::<T>() {
            return None;
        }
        // SAFETY: the page is PAGE bytes from the allocator, 8-aligned, and
        // `slot` is within it (`per_page`).
        Some(unsafe { (pages.table[page] as *mut T).add(slot) })
    }

    fn get(pages: &Pages, id: u32) -> Option<&T> {
        // SAFETY: `ptr` names a record inside a live page; the registry is
        // the only one that hands references to it out.
        Self::ptr(pages, id).map(|p| unsafe { &*p })
    }

    fn get_mut(pages: &mut Pages, id: u32) -> Option<&mut T> {
        // SAFETY: as `get`; `&mut self` on the registry makes it unique.
        Self::ptr(pages, id).map(|p| unsafe { &mut *p })
    }

    fn alloc<A: FnMut(usize) -> *mut u8>(
        &mut self,
        pages: &mut Pages,
        alloc: &mut A,
    ) -> Option<u32> {
        if self.free == NIL {
            let pi = pages.take(alloc)?;
            self.pages[self.n_pages] = pi;
            self.n_pages += 1;
            // Carve the page onto the free list, first slot on top.
            for slot in (0..per_page::<T>()).rev() {
                let id = ((u32::from(pi) << 8) | slot as u32) + 1;
                let next = self.free;
                if let Some(r) = Self::get_mut(pages, id) {
                    r.free(next);
                }
                self.free = id;
            }
        }
        let id = self.free;
        let next = Self::get(pages, id)?.link();
        self.free = next;
        self.live += 1;
        Some(id)
    }

    fn release(&mut self, pages: &mut Pages, id: u32) {
        let next = self.free;
        if let Some(r) = Self::get_mut(pages, id) {
            if r.used() {
                r.free(next);
                self.free = id;
                self.live = self.live.saturating_sub(1);
            }
        }
    }

    /// Every record id in use, in page order.
    fn each(&self, pages: &Pages, mut f: impl FnMut(u32) -> bool) {
        for i in 0..self.n_pages {
            let pi = u32::from(self.pages[i]);
            for slot in 0..per_page::<T>() {
                let id = ((pi << 8) | slot as u32) + 1;
                if Self::get(pages, id).is_some_and(|r| r.used()) && !f(id) {
                    return;
                }
            }
        }
    }
}

/// The registry.
pub struct Registry {
    pages: Pages,
    watches: Slab<Watch>,
    subs: Slab<Sub>,
    index: [u16; MAX_INDEX_PAGES],
    index_pages: usize,
    index_len: usize,
    /// The first deferred subscriber, or [`NIL`].
    pub deferred: u32,
}

/// Why a registry could not take a record.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Full {
    /// The heap gave no page.
    Heap,
    /// A format bound (pages, index pages) is reached.
    Bound,
}

impl Registry {
    pub const fn new() -> Self {
        Registry {
            pages: Pages::new(),
            watches: Slab::new(),
            subs: Slab::new(),
            index: [0; MAX_INDEX_PAGES],
            index_pages: 0,
            index_len: 0,
            deferred: NIL,
        }
    }

    /// Live subscribers and watches, and the pages they hold (telemetry).
    pub fn counts(&self) -> (u32, u32, usize) {
        (self.subs.live, self.watches.live, self.pages.n)
    }

    pub fn watch(&self, id: u32) -> Option<&Watch> {
        Slab::<Watch>::get(&self.pages, id).filter(|w| w.used)
    }

    pub fn sub(&self, id: u32) -> Option<&Sub> {
        Slab::<Sub>::get(&self.pages, id).filter(|s| s.used)
    }

    pub fn sub_mut(&mut self, id: u32) -> Option<&mut Sub> {
        Slab::<Sub>::get_mut(&mut self.pages, id).filter(|s| s.used)
    }

    fn watch_mut(&mut self, id: u32) -> Option<&mut Watch> {
        Slab::<Watch>::get_mut(&mut self.pages, id).filter(|w| w.used)
    }

    // ── The index ────────────────────────────────────────────────────

    fn idx(&self, i: usize) -> u32 {
        let page = self.index[i / IDX_PER_PAGE] as usize;
        // SAFETY: an index page is PAGE bytes, IDX_PER_PAGE u32s.
        unsafe { *(self.pages.table[page] as *const u32).add(i % IDX_PER_PAGE) }
    }

    fn set_idx(&mut self, i: usize, id: u32) {
        let page = self.index[i / IDX_PER_PAGE] as usize;
        // SAFETY: as `idx`.
        unsafe { *(self.pages.table[page] as *mut u32).add(i % IDX_PER_PAGE) = id };
    }

    fn prefix_at(&self, i: usize) -> &[u8] {
        self.watch(self.idx(i)).map_or(&[], |w| w.prefix())
    }

    /// The first index position whose prefix is `>= p`.
    fn lower_bound(&self, p: &[u8]) -> usize {
        let (mut lo, mut hi) = (0, self.index_len);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.prefix_at(mid) < p {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// The watch on exactly `prefix`, if any.
    pub fn find_watch(&self, prefix: &[u8]) -> Option<u32> {
        let i = self.lower_bound(prefix);
        (i < self.index_len && self.prefix_at(i) == prefix).then(|| self.idx(i))
    }

    /// The longest watch that is a proper prefix of `p` (or `p` itself when
    /// `inclusive`), found from `p`'s predecessor along parent links.
    fn longest_prefix_of(&self, p: &[u8], inclusive: bool) -> u32 {
        let i = self.lower_bound(p);
        let start = if inclusive && i < self.index_len && self.prefix_at(i) == p {
            Some(i)
        } else {
            i.checked_sub(1)
        };
        let mut w = match start {
            Some(i) => self.idx(i),
            None => return NIL,
        };
        while let Some(rec) = self.watch(w) {
            let wp = rec.prefix();
            if p.starts_with(wp) && (inclusive || wp.len() < p.len()) {
                return w;
            }
            w = rec.parent;
        }
        NIL
    }

    /// Call `f(sub)` for every subscriber of every watch whose prefix holds
    /// `key`. `f` returning `false` stops the walk.
    pub fn matches(&self, key: &[u8], mut f: impl FnMut(u32) -> bool) {
        let mut w = self.longest_prefix_of(key, true);
        while let Some(rec) = self.watch(w) {
            if key.starts_with(rec.prefix()) {
                let mut s = rec.subs;
                while let Some(sub) = self.sub(s) {
                    if !f(s) {
                        return;
                    }
                    s = sub.next;
                }
            }
            w = rec.parent;
        }
    }

    /// [`matches`](Self::matches), with the registry itself handed to `f`
    /// so it may change the subscriber it is handed — or remove it, and with
    /// it its watch: the walk takes the next subscriber and the parent
    /// before calling `f`. (Removing any other subscriber could end the
    /// walk early.)
    pub fn matches_mut(&mut self, key: &[u8], mut f: impl FnMut(&mut Self, u32)) {
        let mut w = self.longest_prefix_of(key, true);
        while let Some((parent, subs, holds)) = self
            .watch(w)
            .map(|r| (r.parent, r.subs, key.starts_with(r.prefix())))
        {
            if holds {
                let mut s = subs;
                while let Some(next) = self.sub(s).map(|r| r.next) {
                    f(self, s);
                    s = next;
                }
            }
            w = parent;
        }
    }

    fn index_insert<A: FnMut(usize) -> *mut u8>(
        &mut self,
        at: usize,
        id: u32,
        alloc: &mut A,
    ) -> Result<(), Full> {
        if self.index_len == self.index_pages * IDX_PER_PAGE {
            if self.index_pages == MAX_INDEX_PAGES {
                return Err(Full::Bound);
            }
            let pi = self.pages.take(alloc).ok_or(Full::Heap)?;
            self.index[self.index_pages] = pi;
            self.index_pages += 1;
        }
        let mut i = self.index_len;
        while i > at {
            let v = self.idx(i - 1);
            self.set_idx(i, v);
            i -= 1;
        }
        self.set_idx(at, id);
        self.index_len += 1;
        Ok(())
    }

    fn index_remove(&mut self, at: usize) {
        for i in at..self.index_len.saturating_sub(1) {
            let v = self.idx(i + 1);
            self.set_idx(i, v);
        }
        self.index_len = self.index_len.saturating_sub(1);
    }

    /// The watch on `prefix`, made when there is none: its parent found and
    /// the watches under it re-parented — O(the watches under it).
    pub fn watch_for<A: FnMut(usize) -> *mut u8>(
        &mut self,
        prefix: &[u8],
        alloc: &mut A,
    ) -> Result<u32, Full> {
        if prefix.len() > PREFIX_MAX {
            return Err(Full::Bound);
        }
        if let Some(w) = self.find_watch(prefix) {
            return Ok(w);
        }
        // Parent and place are found before the new watch is in the index:
        // a half-made record would break the index's order.
        let at = self.lower_bound(prefix);
        let parent = self.longest_prefix_of(prefix, false);
        let id = self
            .watches
            .alloc(&mut self.pages, alloc)
            .ok_or(Full::Heap)?;
        if let Some(w) = Slab::<Watch>::get_mut(&mut self.pages, id) {
            *w = Watch::EMPTY;
            w.used = true;
            w.plen = prefix.len() as u8;
            copy(&mut w.prefix, prefix);
            w.parent = parent;
        }
        if let Err(e) = self.index_insert(at, id, alloc) {
            self.watches.release(&mut self.pages, id);
            return Err(e);
        }
        // The watches under the new prefix sit right after it in the index;
        // those whose nearest ancestor was above it now have it instead.
        let mut i = at + 1;
        while i < self.index_len && self.prefix_at(i).starts_with(prefix) {
            let wid = self.idx(i);
            let p_old = self.watch(wid).map_or(NIL, |w| w.parent);
            let above = p_old == NIL
                || self
                    .watch(p_old)
                    .is_some_and(|p| p.plen < prefix.len() as u8);
            if above {
                if let Some(w) = self.watch_mut(wid) {
                    w.parent = id;
                }
            }
            i += 1;
        }
        Ok(id)
    }

    /// Drop a watch with no subscribers left: its children take its parent.
    fn drop_watch(&mut self, id: u32) {
        let Some((parent, plen)) = self.watch(id).map(|w| (w.parent, w.plen as usize)) else {
            return;
        };
        let mut prefix = [0u8; PREFIX_MAX];
        if let Some(w) = self.watch(id) {
            copy(&mut prefix, w.prefix());
        }
        let prefix = &prefix[..plen];
        let at = self.lower_bound(prefix);
        let mut i = at + 1;
        while i < self.index_len && self.prefix_at(i).starts_with(prefix) {
            let wid = self.idx(i);
            if let Some(w) = self.watch_mut(wid) {
                if w.parent == id {
                    w.parent = parent;
                }
            }
            i += 1;
        }
        if at < self.index_len && self.idx(at) == id {
            self.index_remove(at);
        }
        self.watches.release(&mut self.pages, id);
    }

    // ── Subscribers ──────────────────────────────────────────────────

    /// A new subscriber on `watch`, filled by `init`.
    pub fn add_sub<A: FnMut(usize) -> *mut u8>(
        &mut self,
        watch: u32,
        alloc: &mut A,
        init: impl FnOnce(&mut Sub),
    ) -> Result<u32, Full> {
        let head = self.watch(watch).ok_or(Full::Bound)?.subs;
        let id = self.subs.alloc(&mut self.pages, alloc).ok_or(Full::Heap)?;
        if let Some(s) = Slab::<Sub>::get_mut(&mut self.pages, id) {
            *s = Sub::EMPTY;
            init(s);
            s.used = true;
            s.watch = watch;
            s.next = head;
            s.defer_next = NIL;
            s.deferred = false;
        }
        if let Some(w) = self.watch_mut(watch) {
            w.subs = id;
        }
        Ok(id)
    }

    /// Remove a subscriber (and its watch, when it was the last).
    pub fn remove_sub(&mut self, id: u32) {
        let Some(watch) = self.sub(id).map(|s| s.watch) else {
            return;
        };
        self.undefer(id);
        // Unlink from the watch's list.
        let next = self.sub(id).map_or(NIL, |s| s.next);
        let head = self.watch(watch).map_or(NIL, |w| w.subs);
        if head == id {
            if let Some(w) = self.watch_mut(watch) {
                w.subs = next;
            }
        } else {
            let mut s = head;
            while let Some(rec) = self.sub(s) {
                if rec.next == id {
                    if let Some(r) = self.sub_mut(s) {
                        r.next = next;
                    }
                    break;
                }
                s = rec.next;
            }
        }
        self.subs.release(&mut self.pages, id);
        if self.watch(watch).is_some_and(|w| w.subs == NIL) {
            self.drop_watch(watch);
        }
    }

    /// Every live subscriber, in page order; `f` returning `false` stops.
    pub fn each_sub(&self, f: impl FnMut(u32) -> bool) {
        self.subs.each(&self.pages, f);
    }

    /// Up to `out.len()` live subscribers after `after` in page order
    /// (`NIL`: from the start), for walks that resume across steps: how many
    /// were written. Pages are never returned, so a subscriber removed since
    /// still marks its place: the walk resumes after its slot.
    pub fn subs_after(&self, after: u32, out: &mut [u32]) -> usize {
        let per = per_page::<Sub>();
        let (start_page, start_slot) = match after.checked_sub(1) {
            None => (0usize, 0usize),
            Some(raw) => {
                let pi = (raw >> 8) as u16;
                let pos = self.subs.pages[..self.subs.n_pages]
                    .iter()
                    .position(|&p| p == pi)
                    .unwrap_or(self.subs.n_pages);
                (pos, (raw & 0xFF) as usize + 1)
            }
        };
        let mut n = 0;
        for i in start_page..self.subs.n_pages {
            let pi = u32::from(self.subs.pages[i]);
            let from = if i == start_page { start_slot } else { 0 };
            for slot in from..per {
                if n == out.len() {
                    return n;
                }
                let id = ((pi << 8) | slot as u32) + 1;
                if self.sub(id).is_some() {
                    out[n] = id;
                    n += 1;
                }
            }
        }
        n
    }

    /// The subscriber after `from` in page order (`NIL` from the start), for
    /// walks that resume across steps: `NIL` past the last, or when `from`
    /// is no longer live ([`subs_after`](Self::subs_after) resumes past a
    /// removed one).
    pub fn next_sub_after(&self, from: u32) -> u32 {
        let mut found = from == NIL;
        let mut out = NIL;
        self.subs.each(&self.pages, |id| {
            if found {
                out = id;
                return false;
            }
            if id == from {
                found = true;
            }
            true
        });
        out
    }

    /// Put a subscriber on the deferred list (a read it named was refused
    /// for now).
    pub fn defer(&mut self, id: u32) {
        let head = self.deferred;
        match self.sub_mut(id) {
            Some(s) if !s.deferred => {
                s.deferred = true;
                s.defer_next = head;
            }
            _ => return,
        }
        self.deferred = id;
    }

    fn undefer(&mut self, id: u32) {
        if !self.sub(id).is_some_and(|s| s.deferred) {
            return;
        }
        let next = self.sub(id).map_or(NIL, |s| s.defer_next);
        if self.deferred == id {
            self.deferred = next;
        } else {
            let mut s = self.deferred;
            while let Some(rec) = self.sub(s) {
                if rec.defer_next == id {
                    if let Some(r) = self.sub_mut(s) {
                        r.defer_next = next;
                    }
                    break;
                }
                s = rec.defer_next;
            }
        }
        if let Some(s) = self.sub_mut(id) {
            s.deferred = false;
            s.defer_next = NIL;
        }
    }

    /// Take the whole deferred list (each subscriber off it), calling `f`
    /// for each.
    pub fn drain_deferred(&mut self, mut f: impl FnMut(&mut Self, u32)) {
        let mut s = core::mem::replace(&mut self.deferred, NIL);
        while s != NIL {
            let next = self.sub(s).map_or(NIL, |r| r.defer_next);
            if let Some(r) = self.sub_mut(s) {
                r.deferred = false;
                r.defer_next = NIL;
            }
            f(self, s);
            s = next;
        }
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

/// A table of `T` records the provider grows on demand from its heap, one
/// record per allocation, up to `N` (a format bound: the record's index is
/// what a handle carries). Records are never returned to the heap; a
/// record no longer in use is the first one a later [`take`](Self::take)
/// reuses. All zero is an empty table.
pub struct Table<T: Copy, const N: usize> {
    recs: [*mut T; N],
    len: usize,
}

impl<T: Copy, const N: usize> Table<T, N> {
    pub const fn new() -> Self {
        Table {
            recs: [core::ptr::null_mut(); N],
            len: 0,
        }
    }

    /// Records made so far (in use or not).
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn get(&self, i: usize) -> Option<&T> {
        let p = *self.recs.get(i).filter(|_| i < self.len)?;
        // SAFETY: a made record: `size_of::<T>()` bytes from the allocator,
        // aligned, live as long as the table.
        Some(unsafe { &*p })
    }

    pub fn get_mut(&mut self, i: usize) -> Option<&mut T> {
        let p = *self.recs.get(i).filter(|_| i < self.len)?;
        // SAFETY: as `get`; `&mut self` makes the reference unique.
        Some(unsafe { &mut *p })
    }

    /// The first record not `used`, or a new one set to `empty` when every
    /// made record is in use: its index. `None` when the heap gives no
    /// record or the table is at `N`.
    pub fn take<A: FnMut(usize) -> *mut u8>(
        &mut self,
        alloc: &mut A,
        empty: T,
        used: impl Fn(&T) -> bool,
    ) -> Option<usize> {
        if let Some(i) = (0..self.len).find(|&i| self.get(i).is_some_and(|r| !used(r))) {
            return Some(i);
        }
        if self.len >= N {
            return None;
        }
        let p = alloc(core::mem::size_of::<T>()) as *mut T;
        if p.is_null() || !(p as usize).is_multiple_of(core::mem::align_of::<T>()) {
            return None;
        }
        // SAFETY: a fresh block of `size_of::<T>()` bytes, aligned.
        unsafe { p.write(empty) };
        self.recs[self.len] = p;
        self.len += 1;
        Some(self.len - 1)
    }

    /// The index of the first made record `f` holds for.
    pub fn position(&self, f: impl Fn(&T) -> bool) -> Option<usize> {
        (0..self.len).find(|&i| self.get(i).is_some_and(&f))
    }

    /// Every made record, in index order.
    pub fn iter(&self) -> impl Iterator<Item = &T> + '_ {
        (0..self.len).filter_map(move |i| self.get(i))
    }

    /// Call `f` on every made record, in index order.
    pub fn each_mut(&mut self, mut f: impl FnMut(&mut T)) {
        for i in 0..self.len {
            if let Some(r) = self.get_mut(i) {
                f(r);
            }
        }
    }
}

impl<T: Copy, const N: usize> Default for Table<T, N> {
    fn default() -> Self {
        Self::new()
    }
}
