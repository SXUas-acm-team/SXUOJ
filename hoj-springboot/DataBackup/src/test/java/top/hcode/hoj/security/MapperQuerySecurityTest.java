package top.hcode.hoj.security;

import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.session.Configuration;
import org.junit.jupiter.api.Test;
import java.io.InputStream;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class MapperQuerySecurityTest {
    private static String sql(String mapper, String method, Map<String,Object> parameters) {
        String resource = "top/hcode/hoj/mapper/xml/" + mapper + ".xml";
        Configuration configuration = new Configuration();
        InputStream stream = MapperQuerySecurityTest.class.getClassLoader().getResourceAsStream(resource);
        assertNotNull(stream, resource);
        new XMLMapperBuilder(stream, configuration, resource, configuration.getSqlFragments()).parse();
        BoundSql bound = configuration.getMappedStatement("top.hcode.hoj.mapper." + mapper + "." + method).getBoundSql(parameters);
        String result = bound.getSql().replaceAll("\\s+", " ");
        assertFalse(result.contains("${")); return result;
    }
    @Test void highestScoreSelectsOneEligibleTieWinnerAndBindsExternalContests() {
        Map<String,Object> parameters = new HashMap<>(); parameters.put("cid", 1L); parameters.put("contestCreatorUid", "owner");
        parameters.put("externalCidList", Arrays.asList(2,3)); parameters.put("isOpenSealRank", true); parameters.put("sealTime", 100L);
        parameters.put("isContainsAfterContestJudge", false); parameters.put("endTime", 1000L);
        String result = sql("ContestRecordMapper", "getOIContestRecordByHighestSubmission", parameters);
        assertTrue(result.contains("better.submit_id < cr.submit_id")); assertTrue(result.contains("cr.time BETWEEN 0 AND ?"));
        assertTrue(result.contains("better.time BETWEEN 0 AND ?")); assertTrue(result.contains("better.cid IN"));
        parameters.put("externalCidList", null); parameters.put("isOpenSealRank", false);
        assertFalse(sql("ContestRecordMapper", "getOIContestRecordByHighestSubmission", parameters).contains(" IN"));
    }
    @Test void acHelperFiltersBeforeDatabasePagination() {
        Map<String,Object> parameters = new HashMap<>(); parameters.put("status", 0); parameters.put("cid", 1L);
        parameters.put("excludedUids", Collections.singletonList("owner")); parameters.put("startTime", new Date()); parameters.put("endTime", new Date());
        String result = sql("ContestRecordMapper", "getACInfoPage", parameters);
        assertTrue(result.contains("earlier.submit_id < c.submit_id")); assertTrue(result.contains("c.uid NOT IN"));
        assertTrue(result.contains("c.submit_time BETWEEN ? AND ?"));
    }
    @Test void privateTrainingSyncRequiresActiveRegistrationAndMembership() {
        Map<String,Object> parameters = new HashMap<>(); parameters.put("uid", "member"); parameters.put("pid", 1L);
        String result = sql("TrainingProblemMapper", "getPrivateTrainingProblemListByPid", parameters);
        assertTrue(result.contains("tr.status = 1 AND t.status = 1")); assertTrue(result.contains("gm.auth IN (3,4,5)"));
    }
    @Test void groupTrainingProgressExcludesContestSubmissionsAndPrivateOutsiders() {
        Map<String,Object> parameters = new HashMap<>(); parameters.put("uid", "member"); parameters.put("gid", 1L); parameters.put("tidList", Collections.singletonList(2L));
        String result = sql("TrainingProblemMapper", "getGroupTrainingListAcceptedCountByUid", parameters);
        assertTrue(result.contains("status = 0 and cid = 0")); assertTrue(result.contains("training_register tr"));
    }
    @Test void heatmapQueryReturnsAtMostOneYearOfDailyAggregates() {
        Map<String,Object> parameters = new HashMap<>(); parameters.put("uid", "member"); parameters.put("username", null);
        String result = sql("JudgeMapper", "getLastYearUserJudgeCounts", parameters);
        assertTrue(result.contains("COUNT(*) AS count")); assertTrue(result.contains("GROUP BY DATE_FORMAT")); assertTrue(result.contains("LIMIT 367"));
    }
}
