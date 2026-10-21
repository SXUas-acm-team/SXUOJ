package top.hcode.hoj.pojo.dto;
import lombok.Data;
import lombok.ToString;
@Data
@ToString(exclude = "password")
public class ContestIdentityInvitationDTO {
    private Long cid;
    private String password;
}
