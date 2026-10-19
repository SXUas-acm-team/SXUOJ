// Keep omitted settings omitted: older admin pages must not erase a policy.
function confidentialFormError(contest) {
  if (!contest.confidential) return null
  if (contest.auth !== 1) return 'Confidential_Requires_Private'
  if (contest.releaseAt != null && (!Number.isFinite(new Date(contest.releaseAt).getTime()) ||
      new Date(contest.releaseAt).getTime() < new Date(contest.endTime).getTime())) {
    return 'Contest_Release_After_End'
  }
  return null
}
module.exports = { confidentialFormError }
