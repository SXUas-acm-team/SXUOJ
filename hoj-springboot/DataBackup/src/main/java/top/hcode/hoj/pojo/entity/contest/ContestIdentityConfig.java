package top.hcode.hoj.pojo.entity.contest;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.ToString;

@Data
@ToString(exclude = "apiKey")
public class ContestIdentityConfig {
    @TableId(value = "cid", type = IdType.INPUT)
    private Long cid;
    private Boolean enabled;
    private Boolean replaceRealname;
    private String starPrefixes;
    private String apiUrl;
    @JsonIgnore
    private String apiKey;
}
