
setblocks <- function(nblocks, N) {
  perblock <- N %/% nblocks
  rem <- N %% nblocks

  common <- rep(seq(nblocks), each = perblock)

  if (rem == 0) return(common)

  return(c(common,
           rep(nblocks, rem)))
}
