package top.hcode.hoj.manager.file;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.FileWriter;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.XmlUtil;
import cn.hutool.core.util.ZipUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.shiro.SecurityUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;
import top.hcode.hoj.common.exception.StatusFailException;
import top.hcode.hoj.dao.problem.LanguageEntityService;
import top.hcode.hoj.dao.problem.ProblemEntityService;
import top.hcode.hoj.exception.ProblemIDRepeatException;
import top.hcode.hoj.pojo.dto.ProblemDTO;
import top.hcode.hoj.pojo.entity.problem.CodeTemplate;
import top.hcode.hoj.pojo.entity.problem.Language;
import top.hcode.hoj.pojo.entity.problem.Problem;
import top.hcode.hoj.pojo.entity.problem.ProblemCase;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.utils.Constants;

import javax.annotation.Resource;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * @Author: Himit_ZH
 * @Date: 2022/3/10 14:44
 * @Description:
 */

@Component
@Slf4j
public class ImportFpsProblemManager {

    private final static List<String> timeUnits = Arrays.asList("ms", "s");
    private static final Map<String, String> fpsMapHOJ = new HashMap<String, String>() {
        {
            put("Python", "Python3");
            put("Go", "Golang");
            put("C", "C");
            put("C++", "C++");
            put("Java", "Java");
            put("C#", "C#");
        }
    };

    @Resource
    private LanguageEntityService languageEntityService;

    @Resource
    private ProblemEntityService problemEntityService;


    /**
     * @param file
     * @MethodName importFpsProblem
     * @Description zip文件导入题目 仅超级管理员可操作
     * @Return
     * @Since 2021/10/06
     */
    public void importFPSProblem(MultipartFile file) throws IOException, StatusFailException {
        String originalName = file.getOriginalFilename();
        String suffix = originalName != null && originalName.contains(".")
                ? originalName.substring(originalName.lastIndexOf('.') + 1)
                : "";
        if (!("xml".equalsIgnoreCase(suffix) || "zip".equalsIgnoreCase(suffix))) {
            throw new StatusFailException("请上传 xml 或 zip 后缀格式的 FPS 题目文件！");
        }
        // 获取当前登录的用户
        AccountProfile userRolesVo = (AccountProfile) SecurityUtils.getSubject().getPrincipal();
        List<ProblemDTO> problemDTOList = new ArrayList<ProblemDTO>();
        // 收集因缺失 test_input 或 test_output 而被跳过的题目标题
        List<String> skippedNoTestCase = new ArrayList<>();
        // 收集无法解析的XML文件名称
        List<String> failedXmlFiles = new ArrayList<>();
        if ("zip".equalsIgnoreCase(suffix)){
            String fileDirId = IdUtil.simpleUUID();
                String testcaseTmpBase = ensureWritableDir(Constants.File.TESTCASE_TMP_FOLDER.getPath(), 
                    System.getProperty("user.home") + File.separator + "hoj" + File.separator + "file" + File.separator + "zip");
                log.info("[FPS-Upload] testcaseTmpBase: {}", testcaseTmpBase);
            String fileDir = testcaseTmpBase + File.separator + fileDirId;
            String filePath = fileDir + File.separator + file.getOriginalFilename();
            // 文件夹不存在就新建
            FileUtil.mkdir(fileDir);
                log.info("[FPS-Upload] unzip dir: {}", fileDir);
            try (InputStream in = file.getInputStream()) {
                FileUtil.writeFromStream(in, filePath);
            } catch (IOException e) {
                log.error("保存上传的 ZIP 文件失败: {}", e.getMessage(), e);
                FileUtil.del(fileDir);
                throw new StatusFailException("服务器异常：FPS题目上传失败！");
            }

            // 将压缩包压缩到指定文件夹
            ZipUtil.unzip(filePath, fileDir);

            // 删除zip文件
            FileUtil.del(filePath);

            // 递归收集 xml 文件
            List<File> allFiles = FileUtil.loopFiles(new File(fileDir));
            List<File> xmlFiles = new ArrayList<>();
            for (File f : allFiles) {
                if (f.isFile() && f.getName().toLowerCase().endsWith(".xml")) {
                    xmlFiles.add(f);
                }
            }
            if (xmlFiles.isEmpty()) {
                FileUtil.del(fileDir);
                throw new StatusFailException("压缩包中未找到任何 XML 文件！");
            }
            for (File xml : xmlFiles) {
                try (java.io.FileInputStream fis = new java.io.FileInputStream(xml)) {
                    try {
                        List<ProblemDTO> parsed = parseFps(fis, userRolesVo.getUsername(), skippedNoTestCase);
                        problemDTOList.addAll(parsed);
                    } catch (StatusFailException e) {
                        String msg = e.getMessage();
                        if (msg != null && msg.startsWith("读取xml失败")) {
                            log.warn("[FPS-Upload] 跳过无法解析的XML文件: {} 原因: {}", xml.getName(), msg);
                            failedXmlFiles.add(xml.getName());
                            // 继续处理其他文件
                        } else {
                            // 非单纯解析失败，抛出维持原逻辑
                            throw e;
                        }
                    }
                }
            }
        }else{
            try (InputStream in = file.getInputStream()) {
                try {
                    problemDTOList = parseFps(in, userRolesVo.getUsername(), skippedNoTestCase);
                } catch (StatusFailException e) {
                    if (e.getMessage() != null && e.getMessage().startsWith("读取xml失败")) {
                        log.warn("[FPS-Upload] 单文件导入解析失败，文件名:{} 原因:{}", originalName, e.getMessage());
                        failedXmlFiles.add(originalName == null ? "uploaded.xml" : originalName);
                    } else {
                        throw e;
                    }
                }
            }
        }

        if (problemDTOList.size() == 0) {
            // 如果所有XML都解析失败
            if (!failedXmlFiles.isEmpty() && skippedNoTestCase.isEmpty()) {
                throw new StatusFailException("未导入任何题目：所有 XML 解析失败 -> " + failedXmlFiles);
            }
            // 如果全部因为没有测试数据而被跳过
            if (!skippedNoTestCase.isEmpty() && failedXmlFiles.isEmpty()) {
                throw new StatusFailException("未导入任何题目：以下题目缺少 test_input 或 test_output -> " + skippedNoTestCase);
            }
            // 混合情况：既有解析失败又有无测试数据
            if (!skippedNoTestCase.isEmpty() || !failedXmlFiles.isEmpty()) {
                throw new StatusFailException("未导入任何题目：解析失败的XML=" + failedXmlFiles + ", 缺少测试数据的题目=" + skippedNoTestCase);
            }
            throw new StatusFailException("警告：未成功导入一道以上的题目，请检查文件格式是否正确！");
        } else {
            HashSet<String> repeatProblemTitleSet = new HashSet<>();
            HashSet<String> failedProblemTitleSet = new HashSet<>();
            int failedCount = 0;
            for (ProblemDTO problemDto : problemDTOList) {
                try {
                    boolean isOk = problemEntityService.adminAddProblem(problemDto);
                    if (!isOk) {
                        failedCount++;
                    }
                } catch (ProblemIDRepeatException e) {
                    repeatProblemTitleSet.add(problemDto.getProblem().getTitle());
                    failedCount++;
                } catch (Exception e) {
                    log.error("", e);
                    failedProblemTitleSet.add(problemDto.getProblem().getTitle());
                    failedCount++;
                }
            }
            if (failedCount > 0) {
                int successCount = problemDTOList.size() - failedCount;
                String errMsg = "[导入结果] 成功数：" + successCount + ", 失败数：" + failedCount +
                        ", 重复失败的题目标题：" + repeatProblemTitleSet;
                if (!failedProblemTitleSet.isEmpty()) {
                    errMsg += "<br/>未知失败的题目标题：" + failedProblemTitleSet;
                }
                if (!skippedNoTestCase.isEmpty()) {
                    errMsg += "<br/>因缺少评测数据被跳过的题目：" + skippedNoTestCase;
                }
                if (!failedXmlFiles.isEmpty()) {
                    errMsg += "<br/>解析失败的XML文件：" + failedXmlFiles;
                }
                throw new StatusFailException(errMsg);
            } else {
                if (!skippedNoTestCase.isEmpty() || !failedXmlFiles.isEmpty()) {
                    String warnComponents = "";
                    if (!skippedNoTestCase.isEmpty()) {
                        warnComponents += "因缺少评测数据跳过的题目：" + skippedNoTestCase;
                    }
                    if (!failedXmlFiles.isEmpty()) {
                        if (!warnComponents.isEmpty()) warnComponents += "；";
                        warnComponents += "解析失败的XML文件：" + failedXmlFiles;
                    }
                    String warnMsg = "[导入完成] " + warnComponents + "，其余题目已成功导入。";
                    throw new StatusFailException(warnMsg);
                }
            }
        }

    }

    private List<ProblemDTO> parseFps(InputStream inputStream, String username, List<String> skippedNoTestCase) throws StatusFailException {

        Document document = null;
        try {
            DocumentBuilderFactory documentBuilderFactory = XmlUtil.createDocumentBuilderFactory();
            documentBuilderFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", false);
            DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
            document = documentBuilder.parse(inputStream);
        } catch (ParserConfigurationException e) {
            log.error("build  DocumentBuilder error:", e);
        } catch (IOException e) {
            log.error("read xml file error:", e);
        } catch (SAXException e) {
            log.error("parse xml file error:", e);
        }
        if (document == null) {
            throw new StatusFailException("读取xml失败，请检查FPS文件格式是否准确！");
        }

        Element rootElement = XmlUtil.getRootElement(document);
        String version = rootElement.getAttribute("version");

        List<ProblemDTO> problemDTOList = new ArrayList<>();

        String fileDirId = IdUtil.simpleUUID();
        String testcaseTmpBase = ensureWritableDir(Constants.File.TESTCASE_TMP_FOLDER.getPath(), 
            System.getProperty("user.home") + File.separator + "hoj" + File.separator + "file" + File.separator + "zip");
        log.info("[FPS-Parse] testcaseTmpBase: {}", testcaseTmpBase);
        String fileDir = testcaseTmpBase + File.separator + fileDirId;
        // 确保基础目录存在
        String markdownDir = ensureWritableDir(Constants.File.MARKDOWN_FILE_FOLDER.getPath(),
                System.getProperty("user.home") + File.separator + "hoj" + File.separator + "file" + File.separator + "md");
        log.info("[FPS-Parse] markdownDir: {}", markdownDir);
        try {
            FileUtil.mkdir(fileDir);
        } catch (Exception e) {
            log.error("创建测试数据临时目录失败: {}", e.getMessage(), e);
            throw new StatusFailException("服务器异常：创建测试数据目录失败！");
        }

        int index = 1;
        for (Element item : XmlUtil.getElements(rootElement, "item")) {

            Problem problem = new Problem();

            problem.setAuthor(username)
                    .setType(0)
                    .setIsUploadCase(true)
                    .setDifficulty(1)
                    .setIsRemoveEndBlank(true)
                    .setOpenCaseResult(true)
                    .setCodeShare(false)
                    .setIsRemote(false)
                    .setAuth(1)
                    .setIsGroup(false);

            Element title = XmlUtil.getElement(item, "title");
            // 标题
            problem.setTitle(title.getTextContent());

            HashMap<String, String> srcMapUrl = new HashMap<>();
            List<Element> images = XmlUtil.getElements(item, "img");
            for (Element img : images) {
                Element srcElement = XmlUtil.getElement(img, "src");
                if (srcElement == null) {
                    continue;
                }
                String src = srcElement.getTextContent();
                String base64 = XmlUtil.getElement(img, "base64").getTextContent();
                String[] split = src.split("\\.");

                byte[] decode = Base64.getDecoder().decode(base64);
                String fileName = IdUtil.fastSimpleUUID() + "." + split[split.length - 1];

                FileUtil.writeBytes(decode, markdownDir + File.separator + fileName);
                srcMapUrl.put(src, Constants.File.IMG_API.getPath() + fileName);
            }

            Element descriptionElement = XmlUtil.getElement(item, "description");
            String description = descriptionElement.getTextContent();
            for (Map.Entry<String, String> entry : srcMapUrl.entrySet()) {
                description = description.replace(entry.getKey(), entry.getValue());
            }
            // 题目描述
            problem.setDescription(description);

            Element inputElement = XmlUtil.getElement(item, "input");
            String input = inputElement.getTextContent();
            for (Map.Entry<String, String> entry : srcMapUrl.entrySet()) {
                input = input.replace(entry.getKey(), entry.getValue());
            }
            // 输入描述
            problem.setInput(input);

            Element outputElement = XmlUtil.getElement(item, "output");
            String output = outputElement.getTextContent();
            for (Map.Entry<String, String> entry : srcMapUrl.entrySet()) {
                output = output.replace(entry.getKey(), entry.getValue());
            }
            // 输出描述
            problem.setOutput(output);

            // 提示
            Element hintElement = XmlUtil.getElement(item, "hint");
            String hint = hintElement.getTextContent();
            for (Map.Entry<String, String> entry : srcMapUrl.entrySet()) {
                hint = hint.replace(entry.getKey(), entry.getValue());
            }
            problem.setHint(hint);

            // 来源
            Element sourceElement = XmlUtil.getElement(item, "source");
            String source = sourceElement.getTextContent();
            problem.setSource(source);

            // ms
            Integer timeLimit = getTimeLimit(version, item);
            problem.setTimeLimit(timeLimit);

            // mb
            Integer memoryLimit = getMemoryLimit(version, item);
            problem.setMemoryLimit(memoryLimit);

            // 题面用例
            List<Element> sampleInputs = XmlUtil.getElements(item, "sample_input");
            List<Element> sampleOutputs = XmlUtil.getElements(item, "sample_output");
            StringBuilder sb = new StringBuilder();
            int exampleCount = Math.min(sampleInputs.size(), sampleOutputs.size());
            for (int i = 0; i < exampleCount; i++) {
                sb.append("<input>").append(sampleInputs.get(i).getTextContent()).append("</input>");
                sb.append("<output>").append(sampleOutputs.get(i).getTextContent()).append("</output>");
            }
            problem.setExamples(sb.toString());


            QueryWrapper<Language> languageQueryWrapper = new QueryWrapper<>();
            languageQueryWrapper.eq("oj", "ME");
            List<Language> languageList = languageEntityService.list(languageQueryWrapper);

            HashMap<String, Long> languageMap = new HashMap<>();
            for (Language language : languageList) {
                languageMap.put(language.getName(), language.getId());
            }

            // 题目模板
            List<Element> templateNodes = XmlUtil.getElements(item, "template");
            List<CodeTemplate> codeTemplates = new ArrayList<>();
            for (Element templateNode : templateNodes) {
                String templateLanguage = templateNode.getAttribute("language");
                String templateCode = templateNode.getTextContent();
                if (templateLanguage == null || templateCode == null) {
                    continue;
                }
                String lang = fpsMapHOJ.get(templateLanguage);
                if (lang != null) {
                    codeTemplates.add(new CodeTemplate()
                            .setCode(templateCode)
                            .setLid(languageMap.get(lang)));
                }

            }

            // spj
            Element spjNode = XmlUtil.getElement(item, "spj");
            if (spjNode != null) {
                String spjLanguage = spjNode.getAttribute("language");
                String spjCode = spjNode.getTextContent();
                if (("C".equals(spjLanguage) || "C++".equals(spjLanguage)) && !StringUtils.isEmpty(spjCode)) {
                    problem.setSpjLanguage(spjLanguage)
                            .setSpjCode(spjCode);
                }
            }

            // 题目评测数据
            List<Element> testInputs = XmlUtil.getElements(item, "test_input");
            List<Element> testOutputs = XmlUtil.getElements(item, "test_output");

            boolean isNotOutputTestCase = CollectionUtils.isEmpty(testOutputs);

            List<ProblemCase> problemSamples = new LinkedList<>();
            String problemTestCaseDir = fileDir + File.separator + index;
            FileUtil.mkdir(problemTestCaseDir);
            log.info("[FPS-Parse] problemTestCaseDir: {}", problemTestCaseDir);
            for (int i = 0; i < testInputs.size(); i++) {
                String infileName = (i + 1) + ".in";
                String outfileName = (i + 1) + ".out";
                File inFile = new File(problemTestCaseDir, infileName);
                File outFile = new File(problemTestCaseDir, outfileName);
                // 确保父目录存在
                FileUtil.mkParentDirs(inFile);
                FileUtil.mkParentDirs(outFile);
                log.info("[FPS-Parse] write files in:{} out:{}", inFile.getAbsolutePath(), outFile.getAbsolutePath());
                try {
                    FileWriter infileWriter = new FileWriter(inFile);
                    FileWriter outfileWriter = new FileWriter(outFile);
                    infileWriter.write(testInputs.get(i).getTextContent());
                    String outContent = (isNotOutputTestCase || i >= testOutputs.size()) ? "" : testOutputs.get(i).getTextContent();
                    outfileWriter.write(outContent);
                } catch (cn.hutool.core.io.IORuntimeException ioEx) {
                    log.error("写入用例文件失败: in={} out={} error={}", inFile.getAbsolutePath(), outFile.getAbsolutePath(), ioEx.getMessage(), ioEx);
                    throw new StatusFailException("服务器异常：写入用例文件失败！");
                }
                problemSamples.add(new ProblemCase()
                        .setInput(infileName).setOutput(outfileName));
            }
            if (CollectionUtils.isEmpty(problemSamples)) {
                log.warn("[FPS-Parse] 题目 '{}' 缺少评测数据（test_input/test_output），已跳过。", problem.getTitle());
                skippedNoTestCase.add(problem.getTitle());
                // 跳过生成 DTO
                continue;
            }
            String mode = Constants.JudgeMode.DEFAULT.getMode();
            if (problem.getSpjLanguage() != null) {
                mode = Constants.JudgeMode.SPJ.getMode();
            }
            ProblemDTO problemDto = new ProblemDTO();
            problemDto.setSamples(problemSamples)
                    .setIsUploadTestCase(true)
                    .setUploadTestcaseDir(problemTestCaseDir)
                    .setLanguages(languageList)
                    .setTags(null)
                    .setJudgeMode(mode)
                    .setProblem(problem)
                    .setCodeTemplates(codeTemplates);

            problemDTOList.add(problemDto);
            index++;
        }
        return problemDTOList;
    }


    private Integer getTimeLimit(String version, Element item) {
        Element timeLimitNode = XmlUtil.getElement(item, "time_limit");
        String timeUnit = timeLimitNode.getAttribute("unit");
        String timeLimit = timeLimitNode.getTextContent();
        int index = timeUnits.indexOf(timeUnit.toLowerCase());
        if ("1.1".equals(version)) {
            if (index == -1) {
                index = 1;
            }
            return Integer.parseInt(timeLimit) * (int) Math.pow(1000, index);
        } else {
            if (index == -1) {
                throw new RuntimeException("Invalid time limit unit:" + timeUnit);
            }
            double tmp = (Double.parseDouble(timeLimit) * Math.pow(1000, index));
            return (int) tmp;
        }
    }

    private Integer getMemoryLimit(String version, Element item) {
        Element memoryLimitNode = XmlUtil.getElement(item, "memory_limit");
        String memoryUnit = memoryLimitNode.getAttribute("unit");
        String memoryLimit = memoryLimitNode.getTextContent();
        String unit = memoryUnit == null ? "" : memoryUnit.trim().toLowerCase();
        if ("1.1".equals(version)) {
            // 旧版本视为 MB 数值
            return Integer.parseInt(memoryLimit);
        }
        if (StringUtils.isEmpty(unit) || "mb".equals(unit)) {
            return (int) Math.ceil(Double.parseDouble(memoryLimit));
        } else if ("kb".equals(unit)) {
            return (int) Math.ceil(Double.parseDouble(memoryLimit) / 1024.0);
        } else {
            throw new RuntimeException("Invalid memory limit unit:" + memoryUnit);
        }
    }

    /**
     * 确保可写目录：优先使用 preferredPath，不可用或创建失败时回退到 fallbackPath。
     */
    private String ensureWritableDir(String preferredPath, String fallbackPath) throws StatusFailException {
        // 先尝试优先目录：创建并写入探针文件验证
        if (isWritable(preferredPath)) {
            return preferredPath;
        } else {
            log.warn("preferredPath 不可写，回退到用户目录: {} -> {}", preferredPath, fallbackPath);
        }
        // 回退目录：创建并写入探针文件验证
        if (isWritable(fallbackPath)) {
            return fallbackPath;
        }
        throw new StatusFailException("服务器异常：无法创建工作目录！");
    }

    private boolean isWritable(String dirPath) {
        try {
            FileUtil.mkdir(dirPath);
            File probe = new File(dirPath, ".probe_" + IdUtil.fastSimpleUUID());
            // Hutool 的 touch 在父目录不存在时会抛错；这里父目录已创建
            boolean created = probe.createNewFile();
            if (created) {
                // 尝试写点内容
                FileWriter fw = new FileWriter(probe);
                fw.write("ok");
                // 清理
                probe.delete();
            }
            return true;
        } catch (Exception e) {
            log.warn("目录不可写: {} , 原因: {}", dirPath, e.getMessage());
            return false;
        }
    }

}