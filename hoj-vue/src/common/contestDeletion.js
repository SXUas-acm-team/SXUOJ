function confirmContestDeletion(view, onConfirmed) {
  const required = view.$i18n.t('m.Current_Account_Password_Required')
  return view.$prompt(
    view.$i18n.t('m.Delete_Contest_Account_Password_Tips'),
    view.$i18n.t('m.Warning'),
    {
      confirmButtonText: view.$i18n.t('m.OK'),
      cancelButtonText: view.$i18n.t('m.Cancel'),
      inputType: 'password',
      inputPlaceholder: view.$i18n.t('m.Current_Account_Password'),
      inputValidator: value => typeof value === 'string' && value.trim().length > 0 ? true : required,
      closeOnClickModal: false,
      type: 'warning',
    }
  ).then(({ value }) => {
    // Keep the original password, including significant leading/trailing spaces.
    if (typeof value !== 'string' || !value.trim()) return
    return onConfirmed(value)
  }).catch(() => {})
}

module.exports = { confirmContestDeletion }
