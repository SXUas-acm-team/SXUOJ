<template>
  <Markdown class="rank-signature-body" :content="content" />
</template>

<script>
import Markdown from '@/components/oj/common/Markdown';

export default {
  name: 'RankSignature',
  components: { Markdown },
  props: {
    content: { type: String, default: '' },
  },
};
</script>

<style scoped>
.rank-signature-body {
  color: inherit;
  background-color: transparent;
  font-size: inherit;
  white-space: normal;
  overflow-wrap: anywhere;
}
.rank-signature-body /deep/ img {
  max-width: 50px;
  max-height: 50px;
  width: auto;
  height: auto;
}
.rank-signature-body /deep/ p {
  margin: 0;
  padding: 0;
}
</style>
