package top.hcode.hoj.security;

import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.multipart.MultipartFile;
import top.hcode.hoj.common.exception.StatusFailException;
import top.hcode.hoj.common.exception.StatusForbiddenException;
import top.hcode.hoj.dao.common.FileEntityService;
import top.hcode.hoj.dao.group.GroupEntityService;
import top.hcode.hoj.dao.problem.ProblemCaseEntityService;
import top.hcode.hoj.dao.problem.ProblemEntityService;
import top.hcode.hoj.manager.file.ImageManager;
import top.hcode.hoj.manager.file.MarkDownFileManager;
import top.hcode.hoj.manager.file.TestCaseManager;
import top.hcode.hoj.pojo.entity.group.Group;
import top.hcode.hoj.pojo.entity.problem.Problem;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.utils.UploadFileUtils;
import top.hcode.hoj.validator.GroupValidator;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class FileBoundarySecondTest {
    @TempDir Path temporary;
    private Subject subject;
    private AccountProfile user;
    private TestCaseManager manager;
    private ProblemEntityService problems;
    private ProblemCaseEntityService cases;
    private GroupEntityService groups;
    private GroupValidator validator;
    private static final long PID = 9223372036854775806L;

    @BeforeEach void setup() {
        subject = mock(Subject.class);
        user = new AccountProfile();
        user.setUid("member"); user.setUsername("member");
        when(subject.getPrincipal()).thenReturn(user);
        ThreadContext.bind(subject);
        manager = new TestCaseManager();
        problems = mock(ProblemEntityService.class);
        cases = mock(ProblemCaseEntityService.class);
        groups = mock(GroupEntityService.class);
        validator = mock(GroupValidator.class);
        ReflectionTestUtils.setField(manager, "problemEntityService", problems);
        ReflectionTestUtils.setField(manager, "problemCaseEntityService", cases);
        ReflectionTestUtils.setField(manager, "groupEntityService", groups);
        ReflectionTestUtils.setField(manager, "groupValidator", validator);
        when(problems.getById(PID)).thenReturn(new Problem().setId(PID).setGid(7L).setAuthor("owner"));
        when(groups.getById(7L)).thenReturn(new Group().setId(7L).setStatus(0));
        when(cases.list(any())).thenReturn(Collections.emptyList());
    }
    @AfterEach void cleanup() { ThreadContext.unbindSubject(); }

    @Test void membershipCannotExposePrivateJudgeInputAndAnswers() {
        when(validator.isGroupMember(user.getUid(), 7L)).thenReturn(true);
        assertThrows(StatusForbiddenException.class, () -> manager.downloadTestcase(PID, new MockHttpServletResponse()));
        verifyNoInteractions(cases);
    }

    @Test void groupAdministratorWhoIsNotTheOwnerCannotDownloadTestcases() {
        when(validator.isGroupAdmin(user.getUid(), 7L)).thenReturn(true);
        assertThrows(StatusForbiddenException.class, () -> manager.downloadTestcase(PID, new MockHttpServletResponse()));
        verifyNoInteractions(cases);
    }

    @Test void groupOwnerCanStillReadTestcases() {
        when(validator.isGroupRoot(user.getUid(), 7L)).thenReturn(true);
        assertAuthorizedUntilEmptyData();
    }

    @Test void problemAuthorCanStillReadTestcases() {
        when(problems.getById(PID)).thenReturn(new Problem().setId(PID).setGid(7L).setAuthor(user.getUsername()));
        assertAuthorizedUntilEmptyData();
    }

    @Test void globalRootCanReadEvenWhenGroupIsBanned() {
        when(subject.hasRole("root")).thenReturn(true);
        when(groups.getById(7L)).thenReturn(new Group().setId(7L).setStatus(1));
        assertAuthorizedUntilEmptyData();
    }

    @Test void bannedGroupRejectsItsOwnProblemAuthor() {
        when(problems.getById(PID)).thenReturn(new Problem().setId(PID).setGid(7L).setAuthor(user.getUsername()));
        when(groups.getById(7L)).thenReturn(new Group().setId(7L).setStatus(1));
        assertThrows(StatusFailException.class, () -> manager.downloadTestcase(PID, new MockHttpServletResponse()));
        verifyNoInteractions(cases);
    }

    @Test void missingProblemAndMissingGroupReturnBusinessErrors() {
        when(problems.getById(PID)).thenReturn(null);
        assertThrows(StatusFailException.class, () -> manager.downloadTestcase(PID, new MockHttpServletResponse()));
        when(problems.getById(PID)).thenReturn(new Problem().setId(PID).setGid(7L));
        when(groups.getById(7L)).thenReturn(null);
        assertThrows(StatusFailException.class, () -> manager.downloadTestcase(PID, new MockHttpServletResponse()));
        verifyNoInteractions(cases);
    }

    @Test void legacyProblemWithoutAuthorDoesNotBypassAuthorizationOrCrash() {
        when(problems.getById(PID)).thenReturn(new Problem().setId(PID).setAuthor(null));
        assertThrows(StatusForbiddenException.class, () -> manager.downloadTestcase(PID, new MockHttpServletResponse()));
        verifyNoInteractions(cases);
    }

    @Test void publicProblemAdministratorRetainsDownloadAccess() {
        when(problems.getById(PID)).thenReturn(new Problem().setId(PID).setAuthor(null));
        when(subject.hasRole("problem_admin")).thenReturn(true);
        assertAuthorizedUntilEmptyData();
    }

    @Test void exactExtensionsSupportUppercaseAndRejectPartialNames() throws Exception {
        assertEquals("jpeg", UploadFileUtils.requireExtension(upload("AVATAR.JPEG"), "jpg", "jpeg", "gif", "png", "webp"));
        assertEquals("zip", UploadFileUtils.requireExtension(upload("cases.ZIP"), "zip"));
        for (String name : new String[]{"image.g", "image.jp", "image.", "image", "image.svg", "image.png.exe"}) {
            assertThrows(StatusFailException.class, () -> UploadFileUtils.requireExtension(upload(name), "jpg", "jpeg", "gif", "png", "webp"), name);
        }
        for (String name : new String[]{"cases.ip", "cases.z", "cases.", "cases"}) {
            assertThrows(StatusFailException.class, () -> UploadFileUtils.requireExtension(upload(name), "zip"), name);
        }
    }

    @Test void emptyUploadAndMissingFilenameFailBeforeStorage() {
        MultipartFile missingName = mock(MultipartFile.class);
        when(missingName.isEmpty()).thenReturn(false);
        assertThrows(StatusFailException.class, () -> UploadFileUtils.requireExtension(missingName, "png"));
        assertThrows(StatusFailException.class, () -> UploadFileUtils.requireExtension(null, "png"));
        assertThrows(StatusFailException.class, () -> UploadFileUtils.requireExtension(new MockMultipartFile("image", "a.png", "image/png", new byte[0]), "png"));
        ImageManager images = new ImageManager();
        assertThrows(StatusFailException.class, () -> images.uploadAvatar(upload("avatar.g")));
        assertThrows(StatusFailException.class, () -> images.uploadAvatar(missingName));
    }

    @Test void testcaseUploadRejectsPartialExtensionAndTraversalBeforeCreatingFiles() {
        when(subject.hasRole("root")).thenReturn(true);
        assertThrows(StatusFailException.class, () -> manager.uploadTestcaseZip(upload("cases.ip"), null, "default"));
        assertThrows(StatusFailException.class, () -> manager.uploadTestcaseZip(upload("../cases.zip"), null, "default"));
        assertThrows(StatusFailException.class, () -> manager.uploadTestcaseZip(upload("..\\cases.zip"), null, "default"));
    }

    @Test void markdownImageUploadUsesTheSameExactExtensionBoundary() {
        when(subject.hasRole("root")).thenReturn(true);
        MarkDownFileManager markdown = new MarkDownFileManager();
        assertThrows(StatusFailException.class, () -> markdown.uploadMDImg(upload("image.g"), 7L));
        assertThrows(StatusFailException.class, () -> markdown.uploadMDImg(new MockMultipartFile("image", "image.png", "image/png", new byte[0]), 7L));
    }

    @Test void groupAdministratorCanDeleteLegacyMarkdownImageWithoutAnUploader() throws Exception {
        Path path = Files.write(temporary.resolve("legacy.png"), new byte[]{1});
        FileEntityService files = mock(FileEntityService.class);
        when(files.getById(1L)).thenReturn(new top.hcode.hoj.pojo.entity.common.File()
                .setId(1L).setType("md").setGid(7L).setUid(null).setFilePath(path.toString()));
        when(validator.isGroupAdmin(user.getUid(), 7L)).thenReturn(true);
        MarkDownFileManager markdown = new MarkDownFileManager();
        ReflectionTestUtils.setField(markdown, "fileEntityService", files);
        ReflectionTestUtils.setField(markdown, "groupValidator", validator);
        markdown.deleteMDImg(1L);
        assertFalse(Files.exists(path));
        verify(files).removeById(1L);
    }

    @Test void memberCannotDeleteAnotherUsersMarkdownImage() throws Exception {
        Path path = Files.write(temporary.resolve("owned.png"), new byte[]{1});
        FileEntityService files = mock(FileEntityService.class);
        when(files.getById(1L)).thenReturn(new top.hcode.hoj.pojo.entity.common.File()
                .setId(1L).setType("md").setGid(7L).setUid("owner").setFilePath(path.toString()));
        MarkDownFileManager markdown = new MarkDownFileManager();
        ReflectionTestUtils.setField(markdown, "fileEntityService", files);
        ReflectionTestUtils.setField(markdown, "groupValidator", validator);
        assertThrows(StatusForbiddenException.class, () -> markdown.deleteMDImg(1L));
        assertTrue(Files.exists(path));
        verify(files, never()).removeById(anyLong());
    }

    private void assertAuthorizedUntilEmptyData() {
        StatusFailException error = assertThrows(StatusFailException.class, () -> manager.downloadTestcase(PID, new MockHttpServletResponse()));
        assertTrue(error.getMessage().contains("数据为空"));
        verify(cases).list(any());
    }
    private MockMultipartFile upload(String filename) {
        return new MockMultipartFile("file", filename, "application/octet-stream", new byte[]{1});
    }
}
