<template>
  <el-col :span="24">
    <el-form-item :label="$t('m.Confidential_Contest')">
      <el-switch :value="!!contest.confidential" @change="setConfidential" />
      <el-alert :title="$t('m.Confidential_Contest_Tips')" type="info" :closable="false" show-icon />
    </el-form-item>
    <template v-if="contest.confidential">
      <el-form-item :label="$t('m.Contest_Release_Time')">
        <el-date-picker v-model="contest.releaseAt" type="datetime" :disabled="busy" :placeholder="$t('m.Contest_Release_Time')" />
      </el-form-item>
      <el-form-item v-if="contest.id" :label="$t('m.Contest_Manual_Publication')">
        <el-button type="warning" :loading="busy" :disabled="busy" @click="changeState(true)">{{ $t('m.Contest_Release_Now') }}</el-button>
        <el-button type="danger" :disabled="busy" @click="changeState(false)">{{ $t('m.Contest_Seal_Now') }}</el-button>
        <div>{{ $t('m.Contest_Manual_Publication_Tips') }}</div>
      </el-form-item>
    </template>
  </el-col>
</template>
<script>
import api from '@/common/api'
import message from '@/common/message'
export default {
  name: 'ContestConfidentialSettings',
  props: {
    contest: { type: Object, required: true },
    groupContest: { type: Boolean, default: false },
  },
  data() { return { busy: false } },
  methods: {
    async changeState(released) {
      if (this.busy || !this.contest.id) return
      this.busy = true
      try {
        await this.$confirm(this.$t(released ? 'm.Contest_Release_Confirm' : 'm.Contest_Seal_Confirm'),
          this.$t('m.Contest_Manual_Publication'), { type: 'warning', closeOnClickModal: false })
        const action = this.groupContest ? 'changeGroupContestConfidentialState' : 'admin_changeContestConfidentialState'
        const response = await api[action](this.contest.id, released)
        this.$set(this.contest, 'releaseAt', response.data.data.releaseAt)
        message.success(this.$t(released ? 'm.Contest_Release_Success' : 'm.Contest_Seal_Success'))
      } catch (error) {
        // The shared API interceptor displays backend failures; cancellation is silent.
      } finally { this.busy = false }
    },
    setConfidential(value) {
      this.$set(this.contest, 'confidential', value)
      if (value) {
        this.$set(this.contest, 'auth', 1)
        this.$set(this.contest, 'openAccountLimit', false)
        if (this.contest.releaseAt === undefined) this.$set(this.contest, 'releaseAt', null)
      }
    },
  },
}
</script>
