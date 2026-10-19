package top.hcode.hoj.pojo.entity.contest;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Date;

/** A row enables confidential access. A null release time keeps the ended contest sealed. */
@Data
@Accessors(chain = true)
public class ContestAccessPolicy {
    @TableId(value = "cid", type = IdType.INPUT)
    private Long cid;
    private Date releaseAt;
}
