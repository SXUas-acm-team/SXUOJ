package top.hcode.hoj.utils;

import org.springframework.web.multipart.MultipartFile;
import top.hcode.hoj.common.exception.StatusFailException;

import java.util.Arrays;
import java.util.Locale;

/** Exact extension checks for uploaded files; filenames are never trusted as paths. */
public final class UploadFileUtils {
    private UploadFileUtils() { }

    public static String requireExtension(MultipartFile file, String... allowed) throws StatusFailException {
        if (file == null || file.isEmpty()) throw new StatusFailException("上传文件不能为空！");
        String name = file.getOriginalFilename();
        if (name == null || name.trim().isEmpty()) throw new StatusFailException("上传文件名不能为空！");
        int dot = name.lastIndexOf('.');
        if (dot < 0 || dot == name.length() - 1) throw new StatusFailException("上传文件格式不支持！");
        String extension = name.substring(dot + 1).toLowerCase(Locale.ROOT);
        if (!Arrays.asList(allowed).contains(extension)) throw new StatusFailException("上传文件格式不支持！");
        return extension;
    }
}
