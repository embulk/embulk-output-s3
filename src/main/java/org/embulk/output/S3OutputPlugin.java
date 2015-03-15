package org.embulk.output;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.Locale;

import org.embulk.config.CommitReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.spi.util.OutputStreamFileOutput;
import org.slf4j.Logger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class S3OutputPlugin implements FileOutputPlugin {
    public interface PluginTask extends Task {
        @Config("path_prefix")
        public String getPathPrefix();

        @Config("file_ext")
        public String getFileNameExtension();

        @Config("sequence_format")
        @ConfigDefault("\".%03d.%02d\"")
        public String getSequenceFormat();

        @Config("bucket")
        public String getBucket();

        @Config("endpoint")
        public String getEndpoint();

        @Config("access_key_id")
        public String getAccessKeyId();

        @Config("secret_access_key")
        public String getSecretAccessKey();
    }

    public static class S3FileOutput extends OutputStreamFileOutput implements
            TransactionalFileOutput {
        public static class Provider implements OutputStreamFileOutput.Provider {
            private final Logger log = Exec.getLogger(S3OutputPlugin.class);

            private int taskIndex;
            private int fileIndex;
            private AmazonS3Client client;
            private String bucket;
            private String pathPrefix;
            private String sequenceFormat;
            private String fileNameExtension;
            private File tempFile;
            private boolean opened = false;

            public static AWSCredentialsProvider getCredentialsProvider(
                    PluginTask task) {
                final AWSCredentials cred = new BasicAWSCredentials(
                        task.getAccessKeyId(), task.getSecretAccessKey());
                return new AWSCredentialsProvider() {
                    @Override
                    public AWSCredentials getCredentials() {
                        return cred;
                    }

                    @Override
                    public void refresh() {
                    }
                };
            }

            private static AmazonS3Client newS3Client(PluginTask task) {
                AWSCredentialsProvider credentials = getCredentialsProvider(task);

                ClientConfiguration config = new ClientConfiguration();
                // TODO: Support more configurations.

                AmazonS3Client client = new AmazonS3Client(credentials, config);
                client.setEndpoint(task.getEndpoint());

                return client;
            }

            public Provider(PluginTask task, int taskIndex) {
                this.taskIndex = taskIndex;
                this.client = newS3Client(task);
                this.bucket = task.getBucket();
                this.pathPrefix = task.getPathPrefix();
                this.sequenceFormat = task.getSequenceFormat();
                this.fileNameExtension = task.getFileNameExtension();
            }

            private static File newTempFile() throws IOException {
                File file = File.createTempFile("embulk-output-s3-", null);
                file.deleteOnExit();
                return file;
            }

            private String buildCurrentPath() {
                String sequence = String.format(sequenceFormat, taskIndex,
                        fileIndex);
                return pathPrefix + sequence + fileNameExtension;
            }

            @Override
            public OutputStream openNext() throws IOException {
                if (opened)
                    return null;

                opened = true;
                tempFile = newTempFile();

                log.info("Writing S3 file '{}'", buildCurrentPath());

                return new FileOutputStream(tempFile);
            }

            @Override
            public void finish() throws IOException {
                if (tempFile == null)
                    return;

                close();
            }

            @Override
            public void close() throws IOException {
                if (tempFile == null)
                    return;

                PutObjectRequest request = new PutObjectRequest(bucket,
                        buildCurrentPath(), tempFile);

                client.putObject(request);

                tempFile = null;

                fileIndex++;
            }
        }

        public S3FileOutput(PluginTask task, int taskIndex) {
            super(new Provider(task, taskIndex));
        }

        @Override
        public void abort() {
        }

        @Override
        public CommitReport commit() {
            CommitReport report = Exec.newCommitReport();
            return report;
        }
    }

    private void validateSequenceFormat(PluginTask task) {
        try {
            @SuppressWarnings("unused")
            String dontCare = String.format(Locale.ENGLISH,
                    task.getSequenceFormat(), 0, 0);
        } catch (IllegalFormatException ex) {
            throw new ConfigException(
                    "Invalid sequence_format: parameter for file output plugin",
                    ex);
        }
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, int taskCount,
            Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);

        validateSequenceFormat(task);

        return resume(task.dump(), taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, int taskCount,
            Control control) {
        control.run(taskSource);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource, int taskCount,
            List<CommitReport> successCommitReports) {
    }

    @Override
    public TransactionalFileOutput open(TaskSource taskSource, int taskIndex) {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        return new S3FileOutput(task, taskIndex);
    }
}
