package top.hcode.hoj.pojo.dto;
import lombok.Data;
import lombok.ToString;
@Data
@ToString(exclude = {"apiKey", "invitationPassword"})
public class ContestIdentityConfigDTO {
    private Boolean enabled;
    private Boolean replaceRealname;
    private String starPrefixes;
    private String apiUrl;
    private String apiKey;
    private String invitationPassword;
    private Boolean clearInvitationPassword;
}
