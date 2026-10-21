package top.hcode.hoj.manager.oj;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.junit.jupiter.api.*;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.test.util.ReflectionTestUtils;
import top.hcode.hoj.common.exception.*;
import top.hcode.hoj.dao.contest.*;
import top.hcode.hoj.mapper.*;
import top.hcode.hoj.pojo.dto.*;
import top.hcode.hoj.pojo.entity.contest.*;
import top.hcode.hoj.pojo.vo.*;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.utils.RedisUtils;
import top.hcode.hoj.validator.ContestValidator;
import top.hcode.hoj.validator.GroupValidator;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ContestIdentityTest {
    private ContestIdentityManager manager;
    private ContestIdentityConfigMapper configs;
    private ContestIdentityBindingMapper bindings;
    private ContestIdentityInvitationMapper invitations;
    private ContestIdentityExemptionMapper exemptions;
    private ContestEntityService contests;
    private ContestAccessPolicyManager policies;
    private ContestIdentityClient client;
    private ContestIdentityBindingWriter writer;
    private IdentitySecretStore secrets;
    private Subject subject;
    private AccountProfile user;
    private Contest contest;
    private ContestIdentityConfig config;
    private ContestIdentityVerifyDTO input;
    private Object oldRedis;
    static void inject(Object object, String field, Object value) { ReflectionTestUtils.setField(object, field, value); }
    @BeforeEach void setup() {
        configs = mock(ContestIdentityConfigMapper.class); bindings = mock(ContestIdentityBindingMapper.class);
        contests = mock(ContestEntityService.class); policies = mock(ContestAccessPolicyManager.class);
        client = mock(ContestIdentityClient.class); writer = mock(ContestIdentityBindingWriter.class); secrets = mock(IdentitySecretStore.class);
        oldRedis = ReflectionTestUtils.getField(RedisUtils.class, "redisTemplate");
        org.springframework.data.redis.core.RedisTemplate<String, Object> template = mock(org.springframework.data.redis.core.RedisTemplate.class);
        org.springframework.data.redis.core.ValueOperations<String, Object> values = mock(org.springframework.data.redis.core.ValueOperations.class);
        when(template.opsForValue()).thenReturn(values); when(values.increment(anyString(), eq(1L))).thenReturn(1L);
        when(template.expire(anyString(), eq(60L), eq(java.util.concurrent.TimeUnit.SECONDS))).thenReturn(true);
        ReflectionTestUtils.setField(RedisUtils.class, "redisTemplate", template);
        RedisUtils redis = new RedisUtils();
        manager = new ContestIdentityManager(); inject(manager, "configs", configs); inject(manager, "bindings", bindings);
        invitations = mock(ContestIdentityInvitationMapper.class); exemptions = mock(ContestIdentityExemptionMapper.class);
        inject(manager, "invitations", invitations); inject(manager, "exemptions", exemptions);
        inject(manager, "contests", contests); inject(manager, "policies", policies); inject(manager, "client", client);
        inject(manager, "writer", writer); inject(manager, "secrets", secrets); inject(manager, "redis", redis);
        inject(manager, "groups", mock(GroupValidator.class));
        user = new AccountProfile(); user.setUid("student"); user.setUsername("student");
        subject = mock(Subject.class); when(subject.getPrincipal()).thenReturn(user); ThreadContext.bind(subject);
        contest = new Contest().setId(7L).setUid("owner").setVisible(true).setIsGroup(false).setAuth(0).setStatus(0).setAllowEndSubmit(false);
        when(contests.getById(7L)).thenReturn(contest); when(contests.getByIdForUpdate(7L)).thenReturn(contest);
        config = new ContestIdentityConfig(); config.setCid(7L); config.setEnabled(true); config.setApiKey("ciphertext");
        config.setApiUrl(ContestIdentityManager.DEFAULT_URL); config.setStarPrefixes("0020"); config.setReplaceRealname(true);
        when(configs.selectById(7L)).thenReturn(config);
        when(secrets.decrypt(7L, config.getApiUrl(), "ciphertext")).thenReturn("private-key");
        input = new ContestIdentityVerifyDTO(); input.setCid(7L); input.setStudentNumber("00201234"); input.setName(" 姓名 ");
    }
    @AfterEach void cleanup() { ThreadContext.unbindSubject(); ReflectionTestUtils.setField(RedisUtils.class, "redisTemplate", oldRedis); }
    @Test void onlyAnExplicitMatchMayCreateBinding() throws Exception {
        when(client.verify(config.getApiUrl(), "private-key", "00201234", "姓名")).thenReturn(true);
        manager.verify(input); verify(writer).bind(config, "student", "00201234", "姓名");
    }
    @Test void mismatchCannotBind() {
        assertThrows(StatusFailException.class, () -> manager.verify(input)); verifyNoInteractions(writer);
    }
    @Test void upstreamFailureCannotBind() throws Exception {
        when(client.verify(anyString(), anyString(), anyString(), anyString())).thenThrow(new StatusFailException("暂时不可用"));
        assertThrows(StatusFailException.class, () -> manager.verify(input)); verifyNoInteractions(writer);
    }
    @Test void loggedOutRequestNeverCallsUpstream() {
        when(subject.getPrincipal()).thenReturn(null);
        assertThrows(StatusForbiddenException.class, () -> manager.verify(input)); verifyNoInteractions(client, writer);
    }
    @Test void privateContestNeedsItsPasswordFirst() {
        contest.setAuth(1); inject(manager, "registrations", mock(ContestRegisterEntityService.class));
        assertThrows(StatusForbiddenException.class, () -> manager.verify(input)); verifyNoInteractions(client, writer);
    }
    @Test void anExistingIdentityCannotBeChangedOrReverified() throws Exception {
        ContestIdentityBinding binding = new ContestIdentityBinding(); binding.setStudentNumber("00201234"); binding.setVerifiedName("姓名");
        when(bindings.selectOne(any(Wrapper.class))).thenReturn(binding);
        manager.verify(input); verifyNoInteractions(client, writer);
        input.setStudentNumber("another"); assertThrows(StatusFailException.class, () -> manager.verify(input)); verifyNoInteractions(client, writer);
    }
    @Test void prefixRulesPreserveLeadingZerosAndRejectNondigits() throws Exception {
        assertEquals("0020,2024", ContestIdentityManager.normalizePrefixes("0020，2024,0020"));
        assertThrows(StatusFailException.class, () -> ContestIdentityManager.normalizePrefixes("2024.*"));
    }
    @Test void ranksUseVerifiedNamesAndMergeManualStars() {
        ContestIdentityBinding binding = new ContestIdentityBinding(); binding.setUid("student"); binding.setStudentNumber("00201234"); binding.setVerifiedName("姓名");
        when(bindings.selectList(any(Wrapper.class))).thenReturn(Collections.singletonList(binding));
        ACMContestRankVO verified = new ACMContestRankVO().setUid("student").setUsername("user").setRealname("untrusted");
        ACMContestRankVO unverified = new ACMContestRankVO().setUid("other").setUsername("other").setRealname("untrusted");
        Map<String, Boolean> stars = new HashMap<>(); stars.put("manual", true);
        manager.decorateRanks(7L, Arrays.asList(verified, unverified), ACMContestRankVO::getUid, ACMContestRankVO::getUsername, ACMContestRankVO::setRealname, stars);
        assertEquals("姓名", verified.getRealname()); assertEquals("", unverified.getRealname());
        assertEquals(new HashSet<>(Arrays.asList("manual", "user")), stars.keySet());
    }
    @Test void disablingNameReplacementPreservesOriginalProfileField() {
        config.setReplaceRealname(false); when(bindings.selectList(any(Wrapper.class))).thenReturn(Collections.emptyList());
        OIContestRankVO row = new OIContestRankVO().setUid("student").setUsername("user").setRealname("profile");
        manager.decorateRanks(7L, Collections.singletonList(row), OIContestRankVO::getUid, OIContestRankVO::getUsername, OIContestRankVO::setRealname, new HashMap<>());
        assertEquals("profile", row.getRealname());
    }
    @Test void publishedContestDoesNotRequireVerification() {
        when(policies.isReleased(contest)).thenReturn(true); assertFalse(manager.required(contest, null));
    }
    @Test void identityOnlyStopsRunningContestSubmissionsAndNeverOrdinaryReads() throws Exception {
        ContestValidator validator = new ContestValidator(); inject(validator, "identityManager", manager);
        inject(validator, "contestAccessPolicyManager", policies); inject(validator, "groupValidator", mock(GroupValidator.class));
        inject(validator, "contestRegisterEntityService", mock(ContestRegisterEntityService.class));
        validator.validateContestAuth(contest, user, false);
        assertThrows(StatusForbiddenException.class, () -> validator.validateJudgeAuth(contest, user, false));
        assertEquals("IDENTITY_REQUIRED", validator.getAccess(contest, user, false).getAccessReason());
        assertTrue(validator.getAccess(contest, user, false).getCanViewProblems());
        assertFalse(validator.getAccess(contest, user, false).getCanSubmit());
        contest.setStatus(1).setAllowEndSubmit(true); when(policies.currentStatus(eq(contest), any())).thenReturn(1);
        validator.validateJudgeAuth(contest, user, false);
        assertFalse(manager.required(contest, user));
        when(subject.hasRole("admin")).thenReturn(true);
        validator.validateContestAuth(contest, user, false); assertEquals("ADMIN", validator.getAccess(contest, user, false).getAccessReason());
    }
    @Test void invitationIsHashedAndGrantIsBoundToItsCurrentContestVersion() throws Exception {
        String hash = ContestInvitationPassword.hash("synthetic-invite");
        assertNotEquals(hash, ContestInvitationPassword.hash("synthetic-invite"));
        assertTrue(ContestInvitationPassword.matches("synthetic-invite", hash));
        assertFalse(ContestInvitationPassword.matches("incorrect-invite", hash));
        ContestIdentityInvitation invitation = new ContestIdentityInvitation(); invitation.setCid(7L); invitation.setPasswordHash(hash);
        when(invitations.selectById(7L)).thenReturn(invitation);
        ContestIdentityInvitationDTO dto = new ContestIdentityInvitationDTO(); dto.setCid(7L); dto.setPassword("incorrect-invite");
        assertThrows(StatusFailException.class, () -> manager.acceptInvitation(dto)); verify(exemptions, never()).insert(any());
        dto.setPassword("synthetic-invite"); manager.acceptInvitation(dto);
        org.mockito.ArgumentCaptor<ContestIdentityExemption> captured = org.mockito.ArgumentCaptor.forClass(ContestIdentityExemption.class);
        verify(exemptions).insert(captured.capture()); assertEquals("student", captured.getValue().getUid());
        when(exemptions.selectOne(any())).thenReturn(captured.getValue()); assertFalse(manager.required(contest, user));
        invitation.setPasswordHash(ContestInvitationPassword.hash("rotated-invite")); assertTrue(manager.required(contest, user));
        assertFalse(new ObjectMapper().writeValueAsString(invitation).contains(hash)); verifyNoInteractions(client, writer);
    }
    @Test void confidentialPasswordAndSealingTakePriorityEvenOverAnExemption() throws Exception {
        ContestValidator validator = new ContestValidator(); inject(validator, "identityManager", manager);
        inject(validator, "contestAccessPolicyManager", policies); inject(validator, "groupValidator", mock(GroupValidator.class));
        ContestRegisterEntityService registrations = mock(ContestRegisterEntityService.class);
        inject(validator, "contestRegisterEntityService", registrations); inject(manager, "registrations", registrations);
        when(policies.isConfidential(contest)).thenReturn(true);
        when(policies.getPolicy(7L)).thenReturn(new ContestAccessPolicy());
        ContestIdentityInvitation invitation = new ContestIdentityInvitation(); invitation.setPasswordHash("version");
        ContestIdentityExemption grant = new ContestIdentityExemption(); grant.setInvitationHash("version");
        when(invitations.selectById(7L)).thenReturn(invitation); when(exemptions.selectOne(any())).thenReturn(grant);
        assertFalse(manager.required(contest, user));
        assertThrows(StatusForbiddenException.class, () -> validator.validateContestAuth(contest, user, false));
        assertEquals("PASSWORD_REQUIRED", validator.getAccess(contest, user, false).getAccessReason());
        when(policies.currentStatus(eq(contest), any())).thenReturn(1);
        assertEquals("SEALED", validator.getAccess(contest, user, false).getAccessReason());
        ContestIdentityInvitationDTO dto = new ContestIdentityInvitationDTO(); dto.setCid(7L); dto.setPassword("synthetic-invite");
        assertThrows(StatusForbiddenException.class, () -> manager.acceptInvitation(dto)); verify(exemptions, never()).insert(any());
    }
    @Test void configDoesNotLeakEncryptedOrPlainSecrets() throws Exception {
        when(subject.hasRole("admin")).thenReturn(true);
        Map<String, Object> result = manager.adminConfig(7L);
        assertEquals(true, result.get("hasApiKey")); assertFalse(result.containsKey("apiKey"));
        assertFalse(new ObjectMapper().writeValueAsString(config).contains("ciphertext"));
    }
    @Test void nonOwnerCannotReadServiceSettings() {
        assertThrows(StatusForbiddenException.class, () -> manager.adminConfig(7L));
    }
    @Test void changingEndpointRequiresKeyReentry() {
        when(subject.hasRole("admin")).thenReturn(true);
        ContestIdentityConfigDTO dto = new ContestIdentityConfigDTO(); dto.setEnabled(true); dto.setReplaceRealname(false);
        dto.setApiUrl("https://other.example/v1/identities/verify-name");
        assertThrows(StatusFailException.class, () -> manager.saveConfig(7L, dto)); verify(configs, never()).updateById(any());
    }
    @Test void writerLocksContestBeforeCheckingBindingAndKeepsOneIdentity() throws Exception {
        ContestIdentityBindingWriter actual = new ContestIdentityBindingWriter();
        inject(actual, "contests", contests); inject(actual, "configs", configs); inject(actual, "bindings", bindings);
        when(bindings.insert(any())).thenReturn(1);
        actual.bind(config, "student", "00201234", "姓名");
        org.mockito.InOrder order = inOrder(contests, configs, bindings);
        order.verify(contests).getByIdForUpdate(7L); order.verify(configs).selectById(7L);
        order.verify(bindings).selectOne(any()); order.verify(bindings).insert(any());
        when(bindings.insert(any())).thenThrow(new DuplicateKeyException("unique"));
        assertThrows(StatusFailException.class, () -> actual.bind(config, "student", "00201234", "姓名"));
    }
    @Test void settingsChangedDuringNetworkRequestCannotCreateBinding() {
        ContestIdentityBindingWriter actual = new ContestIdentityBindingWriter(); inject(actual, "contests", contests);
        inject(actual, "configs", configs); inject(actual, "bindings", bindings);
        ContestIdentityConfig changed = new ContestIdentityConfig(); changed.setEnabled(false);
        when(configs.selectById(7L)).thenReturn(changed);
        assertThrows(StatusFailException.class, () -> actual.bind(config, "student", "00201234", "姓名"));
        verify(bindings, never()).insert(any());
    }
    @Test void ciphertextIsBoundToContestAndEndpointAndUsesRandomNonce() {
        IdentitySecretStore vault = new IdentitySecretStore() { @Override protected byte[] key() { return new byte[16]; } };
        String encrypted = vault.encrypt(7L, "https://example", "private");
        assertEquals("private", vault.decrypt(7L, "https://example", encrypted));
        assertNotEquals(encrypted, vault.encrypt(7L, "https://example", "private"));
        assertThrows(IllegalStateException.class, () -> vault.decrypt(8L, "https://example", encrypted));
        assertThrows(IllegalStateException.class, () -> vault.decrypt(7L, "https://other", encrypted));
    }
    @Test void protocolRejectsAmbiguousOrMockResponses() throws Exception {
        ObjectMapper json = new ObjectMapper(); assertTrue(ContestIdentityClient.matched(json.readTree("{\"matched\":true,\"mode\":\"live\"}")));
        assertFalse(ContestIdentityClient.matched(json.readTree("{\"matched\":false}")));
        for (String value : Arrays.asList("{}", "{\"matched\":\"true\"}", "{\"matched\":true,\"mode\":\"mock\"}")) {
            assertThrows(StatusFailException.class, () -> ContestIdentityClient.matched(json.readTree(value)));
        }
    }
    @Test void endpointsRequireHttpsAndPublicSocketAddresses() throws Exception {
        assertEquals(ContestIdentityManager.DEFAULT_URL, ContestIdentityClient.validateUrl(ContestIdentityManager.DEFAULT_URL));
        assertEquals(ContestIdentityManager.DEFAULT_URL, ContestIdentityClient.validateUrl("https://sf-zt.gwy.fun/"));
        for (String url : Arrays.asList("http://example/v1/identities/verify-name", "https://u:p@example/v1/identities/verify-name", "https://example/v1/identities/verify-name?key=x")) {
            assertThrows(StatusFailException.class, () -> ContestIdentityClient.validateUrl(url));
        }
        for (String host : Arrays.asList("127.0.0.1", "10.1.2.3", "169.254.169.254", "100.64.1.2", "::1")) {
            assertThrows(java.net.UnknownHostException.class, () -> ContestIdentityClient.resolvePublic(host));
        }
    }
    @Test void httpProtocolSendsOnlyRequiredFieldsAndRefusesSuccessBodiesOnErrorStatus() throws Exception {
        org.apache.http.impl.client.CloseableHttpClient transport = mock(org.apache.http.impl.client.CloseableHttpClient.class);
        org.apache.http.client.methods.CloseableHttpResponse response = mock(org.apache.http.client.methods.CloseableHttpResponse.class);
        when(transport.execute(any(org.apache.http.client.methods.HttpUriRequest.class))).thenReturn(response);
        ContestIdentityClient actual = new ContestIdentityClient() { @Override protected org.apache.http.impl.client.CloseableHttpClient newClient() { return transport; } };
        when(response.getStatusLine()).thenReturn(new org.apache.http.message.BasicStatusLine(new org.apache.http.ProtocolVersion("HTTP", 1, 1), 200, "OK"));
        when(response.getEntity()).thenReturn(new org.apache.http.entity.StringEntity("{\"matched\":true}", java.nio.charset.StandardCharsets.UTF_8));
        assertTrue(actual.verify(config.getApiUrl(), "private-key", "00201234", "姓名"));
        org.mockito.ArgumentCaptor<org.apache.http.client.methods.HttpUriRequest> sent = org.mockito.ArgumentCaptor.forClass(org.apache.http.client.methods.HttpUriRequest.class);
        verify(transport).execute(sent.capture());
        assertEquals("private-key", sent.getValue().getFirstHeader("X-API-Key").getValue());
        org.apache.http.client.methods.HttpPost post = (org.apache.http.client.methods.HttpPost) sent.getValue();
        com.fasterxml.jackson.databind.JsonNode body = new ObjectMapper().readTree(org.apache.http.util.EntityUtils.toString(post.getEntity()));
        assertEquals("00201234", body.get("xgh").textValue()); assertEquals("姓名", body.get("name").textValue()); assertEquals(2, body.size());
        for (int status : new int[]{302, 401, 403, 422, 429, 502, 504}) {
            when(response.getStatusLine()).thenReturn(new org.apache.http.message.BasicStatusLine(new org.apache.http.ProtocolVersion("HTTP", 1, 1), status, "Error"));
            assertThrows(StatusFailException.class, () -> actual.verify(config.getApiUrl(), "private-key", "00201234", "姓名"));
        }
        doThrow(new java.net.UnknownHostException("private resolver details")).when(transport).execute(any(org.apache.http.client.methods.HttpUriRequest.class));
        String dnsMessage = assertThrows(StatusFailException.class, () -> actual.verify(config.getApiUrl(), "private-key", "00201234", "姓名")).getMessage();
        assertTrue(dnsMessage.contains("DNS")); assertFalse(dnsMessage.contains("private resolver details"));
        doThrow(new javax.net.ssl.SSLException("private certificate details")).when(transport).execute(any(org.apache.http.client.methods.HttpUriRequest.class));
        assertTrue(assertThrows(StatusFailException.class, () -> actual.verify(config.getApiUrl(), "private-key", "00201234", "姓名")).getMessage().contains("证书"));
    }
    @Test void startupConfigurationParsesWithoutDuplicateKeysAndDisablesSensitiveWireLogs() throws Exception {
        org.yaml.snakeyaml.LoaderOptions options = new org.yaml.snakeyaml.LoaderOptions(); options.setAllowDuplicateKeys(false);
        try (java.io.InputStream input = getClass().getResourceAsStream("/application.yml")) {
            Map<?, ?> root = new org.yaml.snakeyaml.Yaml(new org.yaml.snakeyaml.constructor.SafeConstructor(options)).load(input);
            Map<?, ?> levels = (Map<?, ?>) ((Map<?, ?>) root.get("logging")).get("level");
            assertEquals("OFF", levels.get("org.apache.http.wire")); assertEquals("OFF", levels.get("org.apache.http.headers"));
        }
    }
}
