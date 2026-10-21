package top.hcode.hoj.pojo.entity.contest;
import com.baomidou.mybatisplus.annotation.*;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.ToString;
@Data
@ToString(exclude = "passwordHash")
public class ContestIdentityInvitation {
    @TableId(type = IdType.INPUT) private Long cid;
    @JsonIgnore private String passwordHash;
}
