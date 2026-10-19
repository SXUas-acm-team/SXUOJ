package top.hcode.hoj.mapper;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface ContestProtectedResourceMapper {
    @Insert("INSERT IGNORE INTO contest_protected_resource (cid, resource_hash, resource_path) VALUES (#{cid}, #{hash}, #{path})")
    int remember(@Param("cid") Long cid, @Param("hash") String hash, @Param("path") String path);

    @Select("SELECT cid FROM contest_protected_resource WHERE resource_hash = #{hash} AND resource_path = #{path}")
    List<Long> findContestIds(@Param("hash") String hash, @Param("path") String path);

    @Select("SELECT COUNT(*) FROM contest_protected_resource WHERE cid = #{cid}")
    long countByContestId(@Param("cid") Long cid);

    @Delete("DELETE FROM contest_protected_resource WHERE cid = #{cid}")
    int deleteByContestId(@Param("cid") Long cid);
}
