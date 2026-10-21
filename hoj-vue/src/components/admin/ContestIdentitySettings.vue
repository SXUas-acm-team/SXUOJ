<template>
  <el-col :span="24">
    <el-divider>竞赛入场核验</el-divider>
    <el-alert v-if="!contest.id" title="请先保存比赛，再配置入场核验。" :closable="false" type="info" />
    <div v-else v-loading="loading">
      <el-form-item label="开启竞赛入场核验">
        <el-switch v-model="config.enabled" :disabled="!loaded || saving" />
      </el-form-item>
      <el-alert title="设置独立保存并立即生效。开启后，参赛用户须核验学号和姓名；本场绑定后不能换绑。" :closable="false" type="info" />
      <el-form-item label="核验请求地址">
        <el-input v-model="config.apiUrl" :disabled="!loaded || saving" placeholder="HTTPS 姓名核验接口完整地址" />
      </el-form-item>
      <el-form-item label="核验请求密钥">
        <el-input v-model="config.apiKey" type="password" autocomplete="new-password" :disabled="!loaded || saving"
          :placeholder="config.hasApiKey ? '已配置；留空保留原密钥' : '请输入核验服务 API 密钥'" />
        <div>密钥不会回显。修改请求地址时需重新填写密钥。</div>
      </el-form-item>
      <el-form-item label="学号前缀自动打星">
        <el-input v-model="config.starPrefixes" :disabled="!loaded || saving" placeholder="例如 2024,2025；留空关闭自动打星" />
        <div>已核验学号以任一前缀开头时计为打星，和手工打星名单共同生效。</div>
      </el-form-item>
      <el-form-item label="榜单真实姓名使用核验姓名">
        <el-switch v-model="config.replaceRealname" :disabled="!loaded || saving" />
        <div>替换榜单的真实姓名字段；榜单显示名称选择“真实姓名”后可直接展示。未核验用户的真实姓名留空。</div>
      </el-form-item>
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
  data() { return { config: {}, loading: false, loaded: false, saving: false, requestVersion: 0 } },
  watch: { 'contest.id': { immediate: true, handler() { this.load() } } },
  beforeDestroy() { this.requestVersion++ },
  methods: {
    async load() {
      const version = ++this.requestVersion
      const cid = this.contest.id
      this.config = {}; this.loaded = false
      if (!cid) return
      this.loading = true
      try {
        const response = await api.getContestIdentityConfig(cid)
        if (version !== this.requestVersion || cid !== this.contest.id) return
        this.config = { ...response.data.data, apiKey: '' }; this.loaded = true
      } catch (_) { /* The shared client displays failures. */ }
      finally { if (version === this.requestVersion) this.loading = false }
    },
    async save() {
      if (!this.loaded || this.saving) return
      const cid = this.contest.id
      const version = this.requestVersion
      this.saving = true
      try {
        await api.saveContestIdentityConfig(cid, this.config)
        if (version !== this.requestVersion || cid !== this.contest.id) return
        message.success('核验设置已保存')
        await this.load()
      } catch (_) { /* Never retain a submitted credential after completion. */ }
      finally {
        if (version === this.requestVersion && cid === this.contest.id) this.config.apiKey = ''
        this.saving = false
      }
    },
  },
}
</script>
