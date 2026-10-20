package top.hcode.hoj.pojo.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.ToString;

/** Password belongs to the authenticated operator, never the contest owner or contest itself. */
@Data
@ToString(exclude = "password")
public class DeleteContestDTO {
    private Long cid;
    private Long gid;

    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private String password;
}
