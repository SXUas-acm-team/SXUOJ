<template>
  <el-col :span="24">
    <el-divider>实名参赛校验</el-divider>
    <el-alert v-if="!contest.id" title="请先保存比赛，再配置入场核验。" :closable="false" type="info" />
    <div v-else v-loading="loading">
      <el-form-item label="开启实名参赛校验">
        <el-switch v-model="config.enabled" :disabled="!loaded || saving" />
      </el-form-item>
      <div v-if="config.enabled">
      <el-form-item label="核验请求地址">
        <el-input v-model="config.apiUrl" :disabled="!loaded || saving" placeholder="HTTPS 姓名核验接口完整地址" />
      </el-form-item>
      <el-form-item label="核验请求密钥">
        <el-input v-model="config.apiKey" type="password" autocomplete="new-password" :disabled="!loaded || saving"
          :placeholder="config.hasApiKey ? '已配置；留空保留原密钥' : '请输入核验服务 API 密钥'" />
      </el-form-item>
      <el-form-item label="学号前缀自动打星">
        <el-select v-model="prefixes" multiple filterable allow-create default-first-option :multiple-limit="20"
          :disabled="!loaded || saving" placeholder="输入数字前缀后按回车，可添加多个" style="width:100%">
          <el-option v-for="prefix in prefixes" :key="prefix" :label="prefix" :value="prefix" />
        </el-select>
        <div>已核验学号以任一前缀开头时计为打星，和手工打星名单共同生效。</div>
      </el-form-item>
      <el-form-item label="榜单真实姓名使用核验姓名">
        <el-switch v-model="config.replaceRealname" :disabled="!loaded || saving" />
        <div>替换榜单的真实姓名字段；榜单显示名称选择“真实姓名”后可直接展示。</div>
      </el-form-item>
      <el-form-item label="豁免邀请码">
        <el-input v-model="config.invitationPassword" type="password" autocomplete="new-password" maxlength="64"
          :disabled="!loaded || saving || config.clearInvitationPassword"
          :placeholder="config.hasInvitationPassword ? '已配置；留空保留原邀请码' : '选填，6至64个字符'" />
        <div>供特殊参赛人员豁免实名核验，仍需遵守比赛密码与保密赛规则。更换或清除邀请码后，原有豁免失效。</div>
        <el-checkbox v-if="config.hasInvitationPassword" v-model="config.clearInvitationPassword" :disabled="saving">清除邀请码并撤销现有豁免</el-checkbox>
      </el-form-item>
      </div>
      <el-form-item>
        <el-button type="primary" :loading="saving" :disabled="!loaded || saving" @click="save">保存核验设置</el-button>
        <el-button :disabled="loading || saving" @click="load">重新加载</el-button>
      </el-form-item>
    </div>
  </el-col>
</template>
<script>
import api from '@/common/api'
import message from '@/common/message'
export default {
  name: 'ContestIdentitySettings',
  props: { contest: { type: Object, required: true } },
  data() { return { config: {}, prefixes: [], loading: false, loaded: false, saving: false, requestVersion: 0 } },
  watch: { 'contest.id': { immediate: true, handler() { this.load() } } },
  beforeDestroy() { this.requestVersion++ },
  methods: {
    async load() {
      const version = ++this.requestVersion
      const cid = this.contest.id
      this.config = {}; this.prefixes = []; this.loaded = false
      if (!cid) return
      this.loading = true
      try {
        const response = await api.getContestIdentityConfig(cid)
        if (version !== this.requestVersion || cid !== this.contest.id) return
        this.config = { ...response.data.data, apiKey: '', invitationPassword: '', clearInvitationPassword: false }
        this.prefixes = (this.config.starPrefixes || '').split(/[,，\s]+/).filter(Boolean)
        this.loaded = true
      } catch (_) { /* The shared client displays failures. */ }
      finally { if (version === this.requestVersion) this.loading = false }
    },
    async save() {
      if (!this.loaded || this.saving) return
      const cid = this.contest.id
      const version = this.requestVersion
      this.saving = true
      try {
        await api.saveContestIdentityConfig(cid, { ...this.config, starPrefixes: this.prefixes.join(',') })
        if (version !== this.requestVersion || cid !== this.contest.id) return
        message.success('核验设置已保存')
        await this.load()
      } catch (_) { /* Never retain a submitted credential after completion. */ }
      finally {
        if (version === this.requestVersion && cid === this.contest.id) {
          this.config.apiKey = ''; this.config.invitationPassword = ''
        }
        this.saving = false
      }
    },
  },
}
</script>
