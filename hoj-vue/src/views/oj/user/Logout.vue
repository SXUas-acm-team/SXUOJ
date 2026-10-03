<template>
</template>

<script>
  import api from '@/common/api'

  export default {
    mounted () {
      const completeLogout = () => this.$store.dispatch('clearUserInfoAndToken').then(() =>
        this.$router.replace({ path: '/home' })
      )
      // Password changes revoke the server session before this route is entered.
      return api.logout().then(completeLogout, completeLogout)
    }
  }
</script>
