<template>
  <el-col :span="24">
    <el-form-item :label="$t('m.Confidential_Contest')">
      <el-switch :value="!!contest.confidential" @change="setConfidential" />
      <el-alert :title="$t('m.Confidential_Contest_Tips')" type="info" :closable="false" show-icon />
    </el-form-item>
    <template v-if="contest.confidential">
      <el-form-item :label="$t('m.Contest_Release_Time')">
        <el-date-picker v-model="contest.releaseAt" type="datetime" :placeholder="$t('m.Contest_Release_Time')" />
      </el-form-item>
    </template>
  </el-col>
</template>
<script>
export default {
  name: 'ContestConfidentialSettings',
  props: { contest: { type: Object, required: true } },
  methods: {
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
