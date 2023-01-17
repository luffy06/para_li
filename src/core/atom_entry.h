#ifndef ATOM_ENTRY_H
#define ATOM_ENTRY_H

#include "common.h"

namespace aflipara {
// Atomical value
template <class val_t>
struct AtomicVal {
  union ValUnion;
  typedef ValUnion val_union_t;
  typedef val_t value_type;
  union ValUnion {
    val_t val;
    AtomicVal *ptr;
    ValUnion() {}
    ValUnion(val_t val) : val(val) {}
    ValUnion(AtomicVal *ptr) : ptr(ptr) {}
  };

  // 60 bits for version
  static const uint64_t version_mask = 0x0fffffffffffffff;
  static const uint64_t lock_mask = 0x1000000000000000;
  static const uint64_t removed_mask = 0x2000000000000000;
  static const uint64_t pointer_mask = 0x4000000000000000;

  val_union_t val;
  // lock - removed - is_ptr
  volatile uint64_t status;

  AtomicVal() : status(0) {}
  AtomicVal(val_t val) : val(val), status(0) {}
  AtomicVal(AtomicVal *ptr) : val(ptr), status(0) { set_is_ptr(); }

  bool is_ptr(uint64_t status) { return status & pointer_mask; }
  bool removed(uint64_t status) { return status & removed_mask; }
  bool is_ptr() { return status & pointer_mask; }
  bool removed() {
    if (is_ptr()) return this->val.ptr->removed();
    return status & removed_mask;
  }
  bool locked(uint64_t status) { return status & lock_mask; }
  uint64_t get_version(uint64_t status) { return status & version_mask; }

  void set_is_ptr() { status |= pointer_mask; }
  void unset_is_ptr() { status &= ~pointer_mask; }
  void set_removed() { status |= removed_mask; }
  void lock() {
    while (true) {
      uint64_t old = status;
      uint64_t expected = old & ~lock_mask;  // expect to be unlocked
      uint64_t desired = old | lock_mask;    // desire to lock
      if (likely(cmpxchg((uint64_t *)&this->status, expected, desired) ==
                 expected)) {
        return;
      }
    }
  }
  void unlock() { status &= ~lock_mask; }
  void incr_version() {
    uint64_t version = get_version(status);
    UNUSED(version);
    status++;
    assert(get_version(status) == version + 1);
  }

  // semantics: atomically read the value and the `removed` flag
  bool read(val_t &val) {
    while (true) {
      uint64_t status = this->status;
      memory_fence();
      val_union_t val_union = this->val;
      memory_fence();

      uint64_t current_status = this->status;
      memory_fence();

      if (unlikely(locked(current_status))) {  // check lock
        continue;
      }

      if (likely(get_version(status) ==
                 get_version(current_status))) {  // check version
        if (unlikely(is_ptr(status))) {
          assert(!removed(status));
          return val_union.ptr->read(val);
        } else {
          val = val_union.val;
          return !removed(status);
        }
      }
    }
  }
  bool read_without_lock(val_t &val) {
    while (true) {
      uint64_t status = this->status;
      memory_fence();
      val_union_t val_union = this->val;
      memory_fence();

      uint64_t current_status = this->status;
      memory_fence();

      if (likely(get_version(status) ==
                 get_version(current_status))) {  // check version
        if (unlikely(is_ptr(status))) {
          assert(!removed(status));
          return val_union.ptr->read(val);
        } else {
          val = val_union.val;
          return !removed(status);
        }
      }
    }
  }
  bool update_without_lock(const val_t &val) {
    uint64_t status = this->status;
    bool res;
    if (unlikely(is_ptr(status))) {
      assert(!removed(status));
      res = this->val.ptr->update(val);
    } else if (!removed(status)) {
      this->val.val = val;
      res = true;
    } else {
      res = false;
    }
    memory_fence();
    incr_version();
    memory_fence();
    return res;
  }
  bool update(const val_t &val) {
    lock();
    uint64_t status = this->status;
    bool res;
    if (unlikely(is_ptr(status))) {
      assert(!removed(status));
      res = this->val.ptr->update(val);
    } else if (!removed(status)) {
      this->val.val = val;
      res = true;
    } else {
      res = false;
    }
    memory_fence();
    incr_version();
    memory_fence();
    unlock();
    return res;
  }
  bool remove() {
    lock();
    uint64_t status = this->status;
    bool res;
    if (unlikely(is_ptr(status))) {
      assert(!removed(status));
      res = this->val.ptr->remove();
    } else if (!removed(status)) {
      set_removed();
      res = true;
    } else {
      res = false;
    }
    memory_fence();
    incr_version();
    memory_fence();
    unlock();
    return res;
  }
  void replace_pointer() {
    lock();
    uint64_t status = this->status;
    UNUSED(status);
    assert(is_ptr(status));
    assert(!removed(status));
    if (!val.ptr->read(val.val)) {
      set_removed();
    }
    unset_is_ptr();
    memory_fence();
    incr_version();
    memory_fence();
    unlock();
  }
  bool read_ignoring_ptr(val_t &val) {
    while (true) {
      uint64_t status = this->status;
      memory_fence();
      val_union_t val_union = this->val;
      memory_fence();
      if (unlikely(locked(status))) {
        continue;
      }
      memory_fence();

      uint64_t current_status = this->status;
      if (likely(get_version(status) == get_version(current_status))) {
        val = val_union.val;
        return !removed(status);
      }
    }
  }
  bool update_ignoring_ptr(const val_t &val) {
    lock();
    uint64_t status = this->status;
    bool res;
    if (!removed(status)) {
      this->val.val = val;
      res = true;
    } else {
      res = false;
    }
    memory_fence();
    incr_version();
    memory_fence();
    unlock();
    return res;
  }
  bool remove_ignoring_ptr() {
    lock();
    uint64_t status = this->status;
    bool res;
    if (!removed(status)) {
      set_removed();
      res = true;
    } else {
      res = false;
    }
    memory_fence();
    incr_version();
    memory_fence();
    unlock();
    return res;
  }
};

// template <class val_t>
// struct AtomicVal {
//   val_t val;
//   volatile uint8_t status;

//   AtomicVal() : status(0) {}
//   AtomicVal(val_t val) : val(val), status(0) {}
//   AtomicVal(AtomicVal *ptr) : val(ptr), status(0) { set_is_ptr(); }

//   bool is_ptr(uint64_t status) { return status & pointer_mask; }
//   bool removed(uint64_t status) { return status & removed_mask; }
//   bool is_ptr() { return status & pointer_mask; }
//   bool removed() {
//     if (is_ptr()) return this->val.ptr->removed();
//     return status & removed_mask;
//   }
//   bool locked(uint64_t status) { return status & lock_mask; }
//   uint64_t get_version(uint64_t status) { return status & version_mask; }

//   void set_is_ptr() { status |= pointer_mask; }
//   void unset_is_ptr() { status &= ~pointer_mask; }
//   void set_removed() { status |= removed_mask; }
//   void lock() {
//     while (true) {
//       uint64_t old = status;
//       uint64_t expected = old & ~lock_mask;  // expect to be unlocked
//       uint64_t desired = old | lock_mask;    // desire to lock
//       if (likely(cmpxchg((uint64_t *)&this->status, expected, desired) ==
//                  expected)) {
//         return;
//       }
//     }
//   }
//   void unlock() { status &= ~lock_mask; }
//   void incr_version() {
//     uint64_t version = get_version(status);
//     UNUSED(version);
//     status++;
//     assert(get_version(status) == version + 1);
//   }

//   // semantics: atomically read the value and the `removed` flag
//   bool read(val_t &val) {
//     while (true) {
//       uint64_t status = this->status;
//       memory_fence();
//       val_union_t val_union = this->val;
//       memory_fence();

//       uint64_t current_status = this->status;
//       memory_fence();

//       if (unlikely(locked(current_status))) {  // check lock
//         continue;
//       }

//       if (likely(get_version(status) ==
//                  get_version(current_status))) {  // check version
//         if (unlikely(is_ptr(status))) {
//           assert(!removed(status));
//           return val_union.ptr->read(val);
//         } else {
//           val = val_union.val;
//           return !removed(status);
//         }
//       }
//     }
//   }
//   bool read_without_lock(val_t &val) {
//     while (true) {
//       uint64_t status = this->status;
//       memory_fence();
//       val_union_t val_union = this->val;
//       memory_fence();

//       uint64_t current_status = this->status;
//       memory_fence();

//       if (likely(get_version(status) ==
//                  get_version(current_status))) {  // check version
//         if (unlikely(is_ptr(status))) {
//           assert(!removed(status));
//           return val_union.ptr->read(val);
//         } else {
//           val = val_union.val;
//           return !removed(status);
//         }
//       }
//     }
//   }
//   bool update_without_lock(const val_t &val) {
//     uint64_t status = this->status;
//     bool res;
//     if (unlikely(is_ptr(status))) {
//       assert(!removed(status));
//       res = this->val.ptr->update(val);
//     } else if (!removed(status)) {
//       this->val.val = val;
//       res = true;
//     } else {
//       res = false;
//     }
//     memory_fence();
//     incr_version();
//     memory_fence();
//     return res;
//   }
//   bool update(const val_t &val) {
//     lock();
//     uint64_t status = this->status;
//     bool res;
//     if (unlikely(is_ptr(status))) {
//       assert(!removed(status));
//       res = this->val.ptr->update(val);
//     } else if (!removed(status)) {
//       this->val.val = val;
//       res = true;
//     } else {
//       res = false;
//     }
//     memory_fence();
//     incr_version();
//     memory_fence();
//     unlock();
//     return res;
//   }
//   bool remove() {
//     lock();
//     uint64_t status = this->status;
//     bool res;
//     if (unlikely(is_ptr(status))) {
//       assert(!removed(status));
//       res = this->val.ptr->remove();
//     } else if (!removed(status)) {
//       set_removed();
//       res = true;
//     } else {
//       res = false;
//     }
//     memory_fence();
//     incr_version();
//     memory_fence();
//     unlock();
//     return res;
//   }
//   void replace_pointer() {
//     lock();
//     uint64_t status = this->status;
//     UNUSED(status);
//     assert(is_ptr(status));
//     assert(!removed(status));
//     if (!val.ptr->read(val.val)) {
//       set_removed();
//     }
//     unset_is_ptr();
//     memory_fence();
//     incr_version();
//     memory_fence();
//     unlock();
//   }
//   bool read_ignoring_ptr(val_t &val) {
//     while (true) {
//       uint64_t status = this->status;
//       memory_fence();
//       val_union_t val_union = this->val;
//       memory_fence();
//       if (unlikely(locked(status))) {
//         continue;
//       }
//       memory_fence();

//       uint64_t current_status = this->status;
//       if (likely(get_version(status) == get_version(current_status))) {
//         val = val_union.val;
//         return !removed(status);
//       }
//     }
//   }
//   bool update_ignoring_ptr(const val_t &val) {
//     lock();
//     uint64_t status = this->status;
//     bool res;
//     if (!removed(status)) {
//       this->val.val = val;
//       res = true;
//     } else {
//       res = false;
//     }
//     memory_fence();
//     incr_version();
//     memory_fence();
//     unlock();
//     return res;
//   }
//   bool remove_ignoring_ptr() {
//     lock();
//     uint64_t status = this->status;
//     bool res;
//     if (!removed(status)) {
//       set_removed();
//       res = true;
//     } else {
//       res = false;
//     }
//     memory_fence();
//     incr_version();
//     memory_fence();
//     unlock();
//     return res;
//   }
// };
}

#endif