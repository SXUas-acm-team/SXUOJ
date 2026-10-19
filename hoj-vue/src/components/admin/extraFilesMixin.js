import { parseExtraFiles, isSafeExtraFileName } from '@/common/extraFiles';
import myMessage from '@/common/message';

export default {
  data() {
    return { extraFilesInvalid: false };
  },
  methods: {
    loadExtraFiles() {
      this.extraFilesInvalid = false;
      for (const type of ['user', 'judge']) {
        const property = type + 'ExtraFile';
        const toggle = type === 'user' ? 'addUserExtraFile' : 'addJudgeExtraFile';
        try {
          this[property] = parseExtraFiles(this.problem[property]);
          this[toggle] = Object.keys(this[property]).length > 0;
        } catch (_) {
          this[property] = {};
          this[toggle] = true;
          this.extraFilesInvalid = true;
        }
      }
      if (this.extraFilesInvalid) myMessage.error(this.$i18n.t('m.Invalid_Extra_File_Config'));
    },
    validateExtraFiles() {
      try {
        if (this.extraFilesInvalid) throw new Error('Invalid extra file configuration');
        parseExtraFiles(this.userExtraFile);
        parseExtraFiles(this.judgeExtraFile);
        return true;
      } catch (_) {
        myMessage.error(this.$i18n.t('m.Invalid_Extra_File_Config'));
        return false;
      }
    },
    deleteFile(type, name) {
      const property = type === 'user' ? 'userExtraFile' : 'judgeExtraFile';
      if (this[property]) this.$delete(this[property], name);
    },
    upsertFile(type, name, oldname, content) {
      if (!isSafeExtraFileName(name) || typeof content !== 'string') {
        myMessage.error(this.$i18n.t('m.Invalid_Extra_File_Config'));
        return;
      }
      const property = type === 'user' ? 'userExtraFile' : 'judgeExtraFile';
      if (!this[property]) this[property] = {};
      if (oldname && oldname !== name) this.$delete(this[property], oldname);
      this.$set(this[property], name, content);
    },
  },
};
