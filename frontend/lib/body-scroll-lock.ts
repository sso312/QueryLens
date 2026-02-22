let lockCount = 0

export const lockBodyScroll = () => {
  if (typeof document === "undefined") return
  lockCount += 1
  if (lockCount === 1) {
    document.body.style.overflow = "hidden"
  }
}

export const unlockBodyScroll = () => {
  if (typeof document === "undefined") return
  lockCount = Math.max(0, lockCount - 1)
  if (lockCount === 0) {
    document.body.style.overflow = ""
  }
}

export const resetBodyScrollLock = () => {
  if (typeof document === "undefined") return
  lockCount = 0
  document.body.style.overflow = ""
}
