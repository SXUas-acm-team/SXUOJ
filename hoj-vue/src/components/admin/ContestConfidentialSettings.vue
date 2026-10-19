<template>
  <el-col :span="24">
    <el-form-item :label="$t('m.Confidential_Contest')">
      <el-switch :value="!!contest.confidential" @change="setConfidential" />
      <el-alert :title="$t('m.Confidential_Contest_Tips')" type="info" :closable="false" show-icon />
    </el-form-item>
    <template v-if="contest.confidential">
      <el-form-item :label="$t('m.Contest_Release_Mode')">
        <el-radio-group v-model="releaseMode" @change="changeReleaseMode">
          <el-radio label="manual">{{ $t('m.Manual_Release') }}</el-radio>
          <el-radio label="scheduled">{{ $t('m.Scheduled_Release') }}</el-radio>
        </el-radio-group>
      </el-form-item>
      <el-form-item v-if="releaseMode === 'scheduled'" :label="$t('m.Contest_Release_Time')" required>
        <el-date-picker v-model="contest.releaseAt" type="datetime" :placeholder="$t('m.Contest_Release_Time')" />
      </el-form-item>
      <el-form-item v-if="contest.id && ended" :label="$t('m.Contest_Confidential_Status')">
        <el-tag :type="released ? 'success' : 'warning'">{{ $t(released ? 'm.Contest_Released' : 'm.Contest_Sealed') }}</el-tag>
        <el-button v-if="!released" size="small" type="primary" @click="releaseNow">{{ $t('m.Release_Contest_Now') }}</el-button>
        <el-button v-else size="small" type="warning" @click="sealAgain">{{ $t('m.Seal_Contest_Again') }}</el-button>
        <p>{{ $t('m.Release_Action_Saves_Contest') }}</p>
      </el-form-item>
    </template>
  </el-col>
</template>
<script>
import moment from 'moment'
export default {
  name: 'ContestConfidentialSettings',
  props: { contest: { type: Object, required: true } },
  data() { return { releaseMode: this.contest.releaseAt ? 'scheduled' : 'manual' } },
  computed: {
    ended() { return !!this.contest.endTime && !moment().isBefore(moment(this.contest.endTime)) },
    released() { return this.ended && !!this.contest.releaseAt && !moment().isBefore(moment(this.contest.releaseAt)) },
  },
  watch: {
    'contest.releaseAt'(value) { this.releaseMode = value ? 'scheduled' : 'manual' },
  },
  methods: {
    setConfidential(value) {
      this.$set(this.contest, 'confidential', value)
      if (value) {
        this.$set(this.contest, 'auth', 1)
        this.$set(this.contest, 'openAccountLimit', false)
        if (this.contest.releaseAt === undefined) this.$set(this.contest, 'releaseAt', null)
      }
    },
    changeReleaseMode(value) {
      if (value === 'manual') this.$set(this.contest, 'releaseAt', null)
      else if (!this.contest.releaseAt) this.$set(this.contest, 'releaseAt', moment.max(moment(this.contest.endTime), moment()).add(1, 'day').toISOString())
    },
    releaseNow() {
      this.$set(this.contest, 'releaseAt', new Date().toISOString())
      this.$emit('save')
    },
    sealAgain() {
      this.$set(this.contest, 'releaseAt', null)
      this.$emit('save')
    },
  },
}
</script>
