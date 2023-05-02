package gov.cdc.dex.csv.utils

import java.io.InputStream
import java.io.IOException
import java.io.FileOutputStream
import java.io.File
import java.io.BufferedOutputStream
import java.util.zip.ZipInputStream
import java.util.zip.ZipEntry

private const val BUFFER_SIZE = 4096
fun decompressFileStream(stream:InputStream, destDir:File){
    ZipInputStream(stream).use{ zis ->
        var zipEntry = zis.nextEntry;
        
        while (zipEntry != null) {
            var newFile = newFile(destDir, zipEntry);
            if (zipEntry.isDirectory()) {
                if (!newFile.isDirectory() && !newFile.mkdirs()) {
                    throw IOException("Failed to create directory " + newFile);
                }
            } else {
                // fix for Windows-created archives
                var parent = newFile.getParentFile();
                if (!parent.isDirectory() && !parent.mkdirs()) {
                    throw IOException("Failed to create directory " + parent);
                }
        
                // write file content
                BufferedOutputStream(FileOutputStream(newFile)).use{bos ->
                    val bytesIn = ByteArray(BUFFER_SIZE)
                    var read: Int
                    while (zis.read(bytesIn).also { read = it } != -1) {
                        bos.write(bytesIn, 0, read)
                    }
                }

            }
            zipEntry = zis.getNextEntry();
        }
        zis.closeEntry();
    }
}

private fun newFile( destinationDir:File,  zipEntry:ZipEntry):File {
    val destFile = File(destinationDir, zipEntry.name);

    val destDirPath = destinationDir.canonicalPath;
    val destFilePath = destFile.canonicalPath;

    if (!destFilePath.startsWith(destDirPath + File.separator)) {
        throw IOException("Entry is outside of the target dir: " + zipEntry.getName());
    }

    return destFile;
}