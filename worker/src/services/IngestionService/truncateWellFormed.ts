/**
 * Truncate a string to at most `max` UTF-16 code units WITHOUT emitting lone
 * surrogates.
 *
 * String.prototype.slice cuts at code-unit boundaries, so it can split a
 * surrogate pair (any emoji / astral-plane char is one or more pairs) right
 * at the limit, leaving a dangling high surrogate. JavaScript tolerates that
 * (JSON.stringify happily emits it), but it is NOT valid Unicode — Doris's
 * simdjson rejects the whole row with STRING_ERROR, which under infinite
 * retry wedges the batch forever. This bit for real: a flag emoji split at
 * exactly char 200 of output_trim poisoned an events_full batch.
 *
 * toWellFormed() (Node ≥20) replaces every lone surrogate with U+FFFD — the
 * cut pair AND any lone surrogate the upstream already sent inside the kept
 * window. Preview fields are lossy by definition, so U+FFFD is the correct
 * rendering of "a character was cut here".
 */
export const truncateWellFormed = (s: string, max: number): string =>
  s.slice(0, max).toWellFormed();
