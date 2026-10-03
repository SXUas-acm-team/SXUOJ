package top.hcode.hoj.manager.group.contest;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import org.apache.shiro.SecurityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import top.hcode.hoj.common.exception.StatusFailException;
import top.hcode.hoj.common.exception.StatusForbiddenException;
import top.hcode.hoj.common.exception.StatusNotFoundException;
import top.hcode.hoj.dao.common.AnnouncementEntityService;
import top.hcode.hoj.dao.contest.ContestAnnouncementEntityService;
import top.hcode.hoj.dao.contest.ContestEntityService;
import top.hcode.hoj.dao.group.GroupEntityService;
import top.hcode.hoj.pojo.dto.AnnouncementDTO;
import top.hcode.hoj.pojo.entity.common.Announcement;
import top.hcode.hoj.pojo.entity.contest.Contest;
import top.hcode.hoj.pojo.entity.contest.ContestAnnouncement;
import top.hcode.hoj.pojo.entity.group.Group;
import top.hcode.hoj.pojo.vo.AnnouncementVO;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.validator.CommonValidator;
import top.hcode.hoj.validator.GroupValidator;
import java.util.Objects;

/**
 * @Author: LengYun
 * @Date: 2022/3/11 13:36
 * @Description:
 */
@Component
public class GroupContestAnnouncementManager {

    @Autowired
    private GroupEntityService groupEntityService;

    @Autowired
    private ContestEntityService contestEntityService;

    @Autowired
    private AnnouncementEntityService announcementEntityService;

    @Autowired
    private ContestAnnouncementEntityService contestAnnouncementEntityService;

    @Autowired
    private GroupValidator groupValidator;

    @Autowired
    private CommonValidator commonValidator;

    public IPage<AnnouncementVO> getContestAnnouncementList(Integer limit, Integer currentPage, Long cid) throws StatusNotFoundException, StatusForbiddenException {
        AccountProfile userRolesVo = (AccountProfile) SecurityUtils.getSubject().getPrincipal();

        boolean isRoot = SecurityUtils.getSubject().hasRole("root");

        Contest contest = contestEntityService.getById(cid);

        if (contest == null) {
            throw new StatusNotFoundException("获取失败，该比赛不存在！");
        }

        Long gid = contest.getGid();

        if (gid == null){
            throw new StatusForbiddenException("获取失败，不可获取非团队内的比赛公告！");
        }

        Group group = groupEntityService.getById(gid);

        if (group == null || group.getStatus() == 1 && !isRoot) {
            throw new StatusNotFoundException("获取失败，该团队不存在或已被封禁！");
        }

        if (!userRolesVo.getUid().equals(contest.getUid()) && !isRoot
                && !groupValidator.isGroupRoot(userRolesVo.getUid(), gid)) {
            throw new StatusForbiddenException("对不起，您无权限操作！");
        }

        currentPage = top.hcode.hoj.utils.RequestLimits.pageNumber(currentPage);
        limit = top.hcode.hoj.utils.RequestLimits.pageSize(limit, 10);
        return announcementEntityService.getContestAnnouncement(cid, false, limit, currentPage);
    }

    @Transactional(rollbackFor = Exception.class)
    public void addContestAnnouncement(AnnouncementDTO announcementDto) throws StatusNotFoundException, StatusForbiddenException, StatusFailException {

        commonValidator.validateContent(announcementDto.getAnnouncement().getTitle(), "公告标题", 255);
        commonValidator.validateContentLength(announcementDto.getAnnouncement().getContent(), "公告", 65535);
        commonValidator.validateNotEmpty(announcementDto.getCid(), "比赛ID");

        AccountProfile userRolesVo = (AccountProfile) SecurityUtils.getSubject().getPrincipal();

        boolean isRoot = SecurityUtils.getSubject().hasRole("root");

        Long cid = announcementDto.getCid();

        Contest contest = contestEntityService.getById(cid);

        if (contest == null) {
            throw new StatusNotFoundException("添加失败，该比赛不存在！");
        }

        Long gid = contest.getGid();

        if (gid == null){
            throw new StatusForbiddenException("添加失败，不可操作非团队内的比赛公告！");
        }

        Group group = groupEntityService.getById(gid);

        if (group == null || group.getStatus() == 1 && !isRoot) {
            throw new StatusNotFoundException("添加失败，该团队不存在或已被封禁！");
        }

        if (!userRolesVo.getUid().equals(contest.getUid()) && !isRoot
                && !groupValidator.isGroupRoot(userRolesVo.getUid(), gid)) {
            throw new StatusForbiddenException("对不起，您无权限操作！");
        }

        announcementDto.getAnnouncement().setId(null).setGid(gid).setUid(userRolesVo.getUid())
                .setGmtCreate(null).setGmtModified(null);

        boolean isOk = announcementEntityService.save(announcementDto.getAnnouncement());
        if (isOk) {
            contestAnnouncementEntityService.saveOrUpdate(new ContestAnnouncement()
                    .setAid(announcementDto.getAnnouncement().getId())
                    .setCid(announcementDto.getCid()));
        } else {
            throw new StatusFailException("添加失败！");
        }
    }

    public void updateContestAnnouncement(AnnouncementDTO announcementDto) throws StatusNotFoundException, StatusForbiddenException, StatusFailException {

        commonValidator.validateContent(announcementDto.getAnnouncement().getTitle(), "公告标题", 255);
        commonValidator.validateContentLength(announcementDto.getAnnouncement().getContent(), "公告", 65535);
        commonValidator.validateNotEmpty(announcementDto.getCid(), "比赛ID");

        AccountProfile userRolesVo = (AccountProfile) SecurityUtils.getSubject().getPrincipal();

        boolean isRoot = SecurityUtils.getSubject().hasRole("root");

        Long cid = announcementDto.getCid();

        Contest contest = contestEntityService.getById(cid);

        if (contest == null) {
            throw new StatusNotFoundException("更新失败，该比赛不存在！");
        }

        Long gid = contest.getGid();
        if (gid == null){
            throw new StatusForbiddenException("更新失败，不可操作非团队内的比赛公告！");
        }

        Group group = groupEntityService.getById(gid);

        if (group == null || group.getStatus() == 1 && !isRoot) {
            throw new StatusNotFoundException("更新失败，该团队不存在或已被封禁！");
        }

        if (!userRolesVo.getUid().equals(contest.getUid()) && !isRoot
                && !groupValidator.isGroupRoot(userRolesVo.getUid(), gid)) {
            throw new StatusForbiddenException("对不起，您无权限操作！");
        }

        Announcement requested = announcementDto.getAnnouncement();
        Announcement stored = requireContestAnnouncement(requested.getId(), cid, gid);
        Announcement update = new Announcement().setId(stored.getId()).setTitle(requested.getTitle())
                .setContent(requested.getContent()).setStatus(requested.getStatus());
        boolean isOk = announcementEntityService.updateById(update);
        if (!isOk) {
            throw new StatusFailException("更新失败！");
        }
    }

    public void deleteContestAnnouncement(Long aid, Long cid) throws StatusNotFoundException, StatusForbiddenException, StatusFailException {
        AccountProfile userRolesVo = (AccountProfile) SecurityUtils.getSubject().getPrincipal();

        boolean isRoot = SecurityUtils.getSubject().hasRole("root");

        Contest contest = contestEntityService.getById(cid);

        if (contest == null) {
            throw new StatusNotFoundException("删除失败，该比赛不存在！");
        }

        Long gid = contest.getGid();

        if (gid == null){
            throw new StatusForbiddenException("删除失败，不可操作非团队内的比赛公告！");
        }

        Group group = groupEntityService.getById(gid);

        if (group == null || group.getStatus() == 1 && !isRoot) {
            throw new StatusNotFoundException("删除失败，该团队不存在或已被封禁！");
        }

        if (!userRolesVo.getUid().equals(contest.getUid()) && !isRoot
                && !groupValidator.isGroupRoot(userRolesVo.getUid(), gid)) {
            throw new StatusForbiddenException("对不起，您无权限操作！");
        }

        requireContestAnnouncement(aid, cid, gid);
        boolean isOk = announcementEntityService.removeById(aid);
        if (!isOk) {
            throw new StatusFailException("删除失败！");
        }
    }

    private Announcement requireContestAnnouncement(Long aid, Long cid, Long gid)
            throws StatusNotFoundException, StatusForbiddenException {
        Announcement announcement = aid == null ? null : announcementEntityService.getById(aid);
        if (announcement == null) {
            throw new StatusNotFoundException("该公告不存在！");
        }
        QueryWrapper<ContestAnnouncement> relation = new QueryWrapper<>();
        relation.eq("aid", aid).eq("cid", cid);
        if (!Objects.equals(announcement.getGid(), gid) || contestAnnouncementEntityService.count(relation) == 0) {
            throw new StatusForbiddenException("对不起，该公告不属于当前比赛！");
        }
        return announcement;
    }
}
