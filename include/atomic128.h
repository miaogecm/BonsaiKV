#include <stdint.h>

union atomic_u128 {
  unsigned __int128 val;
  struct {
    volatile uint64_t hi;
    volatile double lo;
  };
}__attribute__((aligned(16)));

// Atomically read a 128 bit unsigned.
__attribute__((always_inline)) inline unsigned __int128 AtomicLoad128(
    register union atomic_u128 *src) {
  union atomic_u128 ret;
  __asm__ __volatile__ (
      "xor %%ecx, %%ecx\n"
      "xor %%eax, %%eax\n"
      "xor %%edx, %%edx\n"
      "xor %%ebx, %%ebx\n"
      "lock cmpxchg16b %2"
      : "=&a"(ret.lo), "=d"(ret.hi)
      : "m"(*src)
      : "cc", "rbx", "rcx" );
  return ret.val;
}

__attribute__((always_inline)) inline int AtomicCAS128(
    volatile union atomic_u128 *src, union atomic_u128 *expected,
    union atomic_u128 desired) {
  int result;
  union atomic_u128 e;
  e.val = expected->val;
  __asm__ __volatile__ (
    "lock cmpxchg16b %1"
    : "=@ccz" ( result ), "+m" ( *src ), "+a"(e.lo), "+d"(e.hi)
    : "c" ( desired.hi ), "b" ( desired.lo )
    : "cc");
  if (!result)
    expected->val = e.val;

  return result;
}