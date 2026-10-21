package top.hcode.hoj.pojo.entity.contest;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.ToString;
@Data
@ToString(exclude = "invitationHash")
public class ContestIdentityExemption {
    private Long cid;
    private String uid;
    @JsonIgnore private String invitationHash;
}
