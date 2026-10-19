package top.hcode.hoj.security;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.context.ApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;
import top.hcode.hoj.dao.problem.*;
import top.hcode.hoj.dao.problem.impl.ProblemEntityServiceImpl;
import top.hcode.hoj.dao.group.GroupEntityService;
import top.hcode.hoj.manager.group.problem.GroupProblemManager;
import top.hcode.hoj.manager.group.contest.GroupContestProblemManager;
import top.hcode.hoj.mapper.ProblemMapper;
import top.hcode.hoj.pojo.dto.ProblemDTO;
import top.hcode.hoj.pojo.entity.group.Group;
import top.hcode.hoj.pojo.entity.problem.*;
import top.hcode.hoj.pojo.vo.ImportProblemVO;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.validator.GroupValidator;
import top.hcode.hoj.validator.ProblemValidator;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Real DAO/manager behavior with mocked DB and explicitly disabled testcase filesystem generation. */
class ProblemAssociationsRegressionTest {
    private ProblemEntityServiceImpl dao;
    private ProblemMapper mapper;
    private ProblemCaseEntityService cases;
    private CodeTemplateEntityService templates;
    private ProblemLanguageEntityService languages;
    private ProblemTagEntityService tags;

    private static void inject(Object target, String field, Object value) { ReflectionTestUtils.setField(target, field, value); }
    @BeforeEach void setup() {
        AccountProfile user = new AccountProfile(); user.setUid("operator"); user.setUsername("operator");
        Subject subject = mock(Subject.class); when(subject.getPrincipal()).thenReturn(user); ThreadContext.bind(subject);
        dao = spy(new ProblemEntityServiceImpl()); mapper = mock(ProblemMapper.class);
        cases = mock(ProblemCaseEntityService.class); templates = mock(CodeTemplateEntityService.class);
        languages = mock(ProblemLanguageEntityService.class); tags = mock(ProblemTagEntityService.class);
        ApplicationContext context = mock(ApplicationContext.class); when(context.getBean(ProblemEntityServiceImpl.class)).thenReturn(dao);
        inject(dao, "problemMapper", mapper); inject(dao, "problemCaseEntityService", cases);
        inject(dao, "codeTemplateEntityService", templates); inject(dao, "problemLanguageEntityService", languages);
        inject(dao, "problemTagEntityService", tags); inject(dao, "tagEntityService", mock(TagEntityService.class));
        inject(dao, "applicationContext", context);
        doNothing().when(dao).initHandTestCase(anyString(), anyString(), anyString(), anyLong(), anyList());
        doNothing().when(dao).initUploadTestCase(anyString(), anyString(), anyString(), anyLong(), nullable(String.class), anyList());
        when(cases.listByMap(anyMap())).thenReturn(Collections.emptyList());
        when(templates.listByMap(anyMap())).thenReturn(Collections.emptyList());
        when(languages.listByMap(anyMap())).thenReturn(Collections.emptyList());
        when(tags.listByMap(anyMap())).thenReturn(Collections.emptyList());
        when(cases.saveOrUpdateBatch(anyCollection())).thenReturn(true); when(cases.saveBatch(anyCollection())).thenReturn(true);
        when(cases.removeByIds(anyCollection())).thenReturn(true);
        when(templates.saveOrUpdateBatch(anyCollection())).thenReturn(true); when(templates.removeByIds(anyCollection())).thenReturn(true);
        when(languages.saveOrUpdateBatch(anyCollection())).thenReturn(true);
        when(mapper.updateById(any(Problem.class))).thenReturn(1);
        when(mapper.selectById(20L)).thenReturn(problem());
        when(mapper.insert(any(Problem.class))).thenAnswer(inv -> { Problem created = inv.getArgument(0);
            assertNull(created.getId()); created.setId(20L); return 1;
        });
    }
    @AfterEach void cleanup() { ThreadContext.unbindSubject(); }
    private Problem problem() {
        return new Problem().setId(20L).setProblemId("TEAM1000").setTitle("test").setType(0)
                .setIsRemote(false).setIsGroup(true).setGid(7L).setAuthor("owner").setJudgeMode("default").setJudgeCaseMode("default");
    }
    private ProblemDTO dto() {
        return new ProblemDTO().setProblem(problem()).setJudgeMode("default").setIsUploadTestCase(false)
                .setCodeTemplates(new ArrayList<>()).setSamples(new ArrayList<>()).setTags(new ArrayList<>()).setLanguages(new ArrayList<>());
    }
    private void assertNoChildWrites() {
        verify(templates, never()).saveOrUpdateBatch(anyCollection()); verify(templates, never()).removeByIds(anyCollection());
        verify(cases, never()).saveOrUpdateBatch(anyCollection()); verify(cases, never()).saveBatch(anyCollection());
        verify(cases, never()).removeByIds(anyCollection()); verify(mapper, never()).updateById(any(Problem.class));
    }
    @Test void newManualProblemCannotOverwriteExistingCaseOrTemplateIds() {
        ProblemDTO request = dto().setCodeTemplates(Collections.singletonList(new CodeTemplate().setId(88).setPid(999L).setCode("code")))
                .setSamples(Collections.singletonList(new ProblemCase().setId(77L).setPid(999L).setInput("in").setOutput("out")));
        assertTrue(dao.adminAddProblem(request));
        ArgumentCaptor<Collection> savedTemplates = ArgumentCaptor.forClass(Collection.class);
        ArgumentCaptor<Collection> savedCases = ArgumentCaptor.forClass(Collection.class);
        verify(templates).saveOrUpdateBatch(savedTemplates.capture()); verify(cases).saveOrUpdateBatch(savedCases.capture());
        CodeTemplate template = (CodeTemplate) savedTemplates.getValue().iterator().next();
        ProblemCase sample = (ProblemCase) savedCases.getValue().iterator().next();
        assertNull(template.getId()); assertEquals(Long.valueOf(20), template.getPid()); assertEquals("code", template.getCode());
        assertNull(sample.getId()); assertEquals(Long.valueOf(20), sample.getPid()); assertEquals("in", sample.getInput());
    }
    @Test void newUploadedProblemAlsoClearsClientCasePrimaryKeys() {
        ProblemDTO request = dto().setIsUploadTestCase(true).setSamples(Collections.singletonList(
                new ProblemCase().setId(77L).setPid(999L).setInput("1.in").setOutput("1.out")));
        assertTrue(dao.adminAddProblem(request));
        ArgumentCaptor<Collection> savedCases = ArgumentCaptor.forClass(Collection.class); verify(cases).saveOrUpdateBatch(savedCases.capture());
        ProblemCase sample = (ProblemCase) savedCases.getValue().iterator().next();
        assertNull(sample.getId()); assertEquals(Long.valueOf(20), sample.getPid());
    }
    @Test void templateFromAnotherProblemIsRejectedBeforeAnyChildWrite() {
        ProblemDTO request = dto().setCodeTemplates(Collections.singletonList(new CodeTemplate().setId(88).setPid(20L).setCode("evil")));
        assertThrows(IllegalArgumentException.class, () -> dao.adminUpdateProblem(request)); assertNoChildWrites();
    }
    @Test void caseFromAnotherProblemIsRejectedBeforeAnyChildWrite() {
        ProblemDTO request = dto().setSamples(Collections.singletonList(new ProblemCase().setId(77L).setPid(20L).setInput("evil").setOutput("evil")));
        assertThrows(IllegalArgumentException.class, () -> dao.adminUpdateProblem(request)); assertNoChildWrites();
    }
    @Test void validExistingChildrenRemainBoundToTheirOriginalProblem() {
        when(templates.listByMap(anyMap())).thenReturn(Collections.singletonList(new CodeTemplate().setId(8).setPid(20L)));
        when(cases.listByMap(anyMap())).thenReturn(Collections.singletonList(new ProblemCase().setId(7L).setPid(20L).setInput("old").setOutput("old")));
        ProblemDTO request = dto().setCodeTemplates(Collections.singletonList(new CodeTemplate().setId(8).setPid(999L).setCode("new")))
                .setSamples(Collections.singletonList(new ProblemCase().setId(7L).setPid(999L).setInput("new").setOutput("new")));
        assertTrue(dao.adminUpdateProblem(request));
        ArgumentCaptor<Collection> savedTemplates = ArgumentCaptor.forClass(Collection.class), savedCases = ArgumentCaptor.forClass(Collection.class);
        verify(templates).saveOrUpdateBatch(savedTemplates.capture()); verify(cases).saveOrUpdateBatch(savedCases.capture());
        CodeTemplate template = (CodeTemplate) savedTemplates.getValue().iterator().next(); ProblemCase sample = (ProblemCase) savedCases.getValue().iterator().next();
        assertEquals(Integer.valueOf(8), template.getId()); assertEquals(Long.valueOf(20), template.getPid());
        assertEquals(Long.valueOf(7), sample.getId()); assertEquals(Long.valueOf(20), sample.getPid());
    }
    @Test void validNewChildrenCanStillBeAddedDuringProblemEdit() {
        ProblemDTO request = dto().setCodeTemplates(Collections.singletonList(new CodeTemplate().setPid(999L).setCode("new")))
                .setSamples(Collections.singletonList(new ProblemCase().setPid(999L).setInput("new").setOutput("new")));
        assertTrue(dao.adminUpdateProblem(request));
        ArgumentCaptor<Collection> savedCases = ArgumentCaptor.forClass(Collection.class); verify(cases).saveBatch(savedCases.capture());
        ProblemCase sample = (ProblemCase) savedCases.getValue().iterator().next(); assertNull(sample.getId()); assertEquals(Long.valueOf(20), sample.getPid());
        verify(templates).saveOrUpdateBatch(anyCollection());
    }
    private ImportProblemVO export(Problem problem) {
        when(mapper.selectById(20L)).thenReturn(problem); when(templates.list(any(Wrapper.class))).thenReturn(Collections.emptyList());
        when(tags.list(any(Wrapper.class))).thenReturn(Collections.emptyList()); when(languages.list(any(Wrapper.class))).thenReturn(Collections.emptyList());
        return dao.buildExportProblem(20L, Collections.emptyList(), new HashMap<>(), new HashMap<>());
    }
    @Test void exportKeepsJudgeOnlyFilesOutOfUserFileMap() {
        ImportProblemVO result = export(problem().setUserExtraFile("{\"public.h\":\"public\"}")
                .setJudgeExtraFile("{\"secret.h\":\"secret\"}"));
        assertEquals(Collections.singletonMap("public.h", "public"), result.getUserExtraFile());
        assertEquals(Collections.singletonMap("secret.h", "secret"), result.getJudgeExtraFile());
        assertFalse(result.getUserExtraFile().containsKey("secret.h"));
    }
    @Test void exportWithOnlyJudgeFilesDoesNotPopulateUserFiles() {
        ImportProblemVO result = export(problem().setJudgeExtraFile("{\"secret.h\":\"secret\"}"));
        assertNull(result.getUserExtraFile()); assertEquals("secret", result.getJudgeExtraFile().get("secret.h"));
    }

    private GroupEntityService groups() {
        GroupEntityService groups = mock(GroupEntityService.class); when(groups.getById(7L))
                .thenReturn(new Group().setId(7L).setShortName("TEAM").setStatus(0)); return groups;
    }
    private GroupValidator groupValidator() {
        GroupValidator validator = mock(GroupValidator.class); when(validator.isGroupAdmin("operator", 7L)).thenReturn(true);
        when(validator.isGroupRoot("operator", 7L)).thenReturn(true); return validator;
    }
    private GroupProblemManager groupProblemManager(ProblemEntityService problems) {
        GroupProblemManager manager = new GroupProblemManager(); inject(manager, "problemEntityService", problems);
        inject(manager, "groupEntityService", groups()); inject(manager, "groupValidator", groupValidator());
        inject(manager, "problemValidator", mock(ProblemValidator.class)); return manager;
    }
    @Test void newTeamProblemAuthorComesFromAuthenticatedUser() throws Exception {
        ProblemEntityService problems = mock(ProblemEntityService.class); when(problems.adminAddProblem(any(ProblemDTO.class))).thenReturn(true);
        groupProblemManager(problems).addProblem(dto().setProblem(problem().setAuthor("victim")));
        ArgumentCaptor<ProblemDTO> saved = ArgumentCaptor.forClass(ProblemDTO.class); verify(problems).adminAddProblem(saved.capture());
        assertEquals("operator", saved.getValue().getProblem().getAuthor()); assertEquals("operator", saved.getValue().getProblem().getModifiedUser());
    }
    @Test void teamProblemEditCannotTransferAuthorViaRequest() throws Exception {
        ProblemEntityService problems = mock(ProblemEntityService.class); when(problems.getById(20L)).thenReturn(problem());
        when(problems.getOne(any(Wrapper.class))).thenReturn(problem()); when(problems.adminUpdateProblem(any(ProblemDTO.class))).thenReturn(true);
        groupProblemManager(problems).updateProblem(dto().setProblem(problem().setAuthor("victim")));
        ArgumentCaptor<ProblemDTO> saved = ArgumentCaptor.forClass(ProblemDTO.class); verify(problems).adminUpdateProblem(saved.capture());
        assertEquals("owner", saved.getValue().getProblem().getAuthor()); assertEquals("operator", saved.getValue().getProblem().getModifiedUser());
    }
    @Test void newTeamContestProblemAlsoUsesAuthenticatedAuthor() throws Exception {
        ProblemEntityService problems = mock(ProblemEntityService.class); when(problems.adminAddProblem(any(ProblemDTO.class))).thenReturn(true);
        GroupContestProblemManager manager = new GroupContestProblemManager(); inject(manager, "problemEntityService", problems);
        inject(manager, "groupEntityService", groups()); inject(manager, "groupValidator", groupValidator()); inject(manager, "problemValidator", mock(ProblemValidator.class));
        manager.addProblem(dto().setProblem(problem().setAuthor("victim")));
        ArgumentCaptor<ProblemDTO> saved = ArgumentCaptor.forClass(ProblemDTO.class); verify(problems).adminAddProblem(saved.capture());
        assertEquals("operator", saved.getValue().getProblem().getAuthor());
    }
}
