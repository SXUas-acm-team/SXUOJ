package top.hcode.hoj.pojo.entity.contest;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.Data;
import java.util.Date;

@Data
public class ContestIdentityBinding {
    @TableId(type = IdType.AUTO)
    private Long id;
    private Long cid;
    private String uid;
    private String studentNumber;
    private String verifiedName;
    private Date verifiedAt;
}
