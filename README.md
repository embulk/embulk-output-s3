# S3 file output plugin for Embulk

## Developers

* Manabu Takayama <learn.libra@gmail.com>
* toyama hiroshi <toyama0919@gmail.com>
* Civitaspo <civitaspo@gmail.com>

## Overview

* **Plugin type**: file output
* **Load all or nothing**: no
* **Resume supported**: yes
* **Cleanup supported**: yes

## Configuration

- **path_prefix**: prefix of target keys (string, required)
- **file_ext**: suffix of target keys (string, required)
- **sequence_format**: format for sequence part of target keys (string, default: '.%03d.%02d')
- **bucket**: S3 bucket name (string, required)
- **endpoint**: S3 endpoint login user name (string, optional)
- **access_key_id**: AWS access key id. This parameter is required when your agent is not running on EC2 instance with an IAM Role. (string, defualt: null)
- **secret_access_key**: AWS secret key. This parameter is required when your agent is not running on EC2 instance with an IAM Role. (string, defualt: null)
- **tmp_path**: temporary file directory. If null, it is associated with the default FileSystem. (string, default: null)
- **tmp_path_prefix**: prefix of temporary files (string, default: 'embulk-output-s3-')
- **canned_acl**: canned access control list for created objects ([enum](#cannedaccesscontrollist), default: null)
- [Deprecated] **proxy_host**: proxy host to use when accessing AWS S3 via proxy. (string, default: null )
- [Deprecated] **proxy_port**: proxy port to use when accessing AWS S3 via proxy. (string, default: null )
- **http_proxy**: http proxy configuration to use when accessing AWS S3 via http proxy. (optional)
  - **host**: proxy host (string, required)
  - **port**: proxy port (int, optional)
  - **https**: use https or not (boolean, default true)
  - **user**: proxy user (string, optional)
  - **password**: proxy password (string, optional)

- **auth_method**: name of mechanism to authenticate requests (basic, env, instance, profile, properties, anonymous, or session. default: basic)

    - "basic": uses access_key_id and secret_access_key to authenticate.

        - **access_key_id**: AWS access key ID (string, required)

        - **secret_access_key**: AWS secret access key (string, required)

    - "env": uses AWS_ACCESS_KEY_ID (or AWS_ACCESS_KEY) and AWS_SECRET_KEY (or AWS_SECRET_ACCESS_KEY) environment variables.

    - "instance": uses EC2 instance profile.

    - "profile": uses credentials written in a file. Format of the file is as following, where `[...]` is a name of profile.

        - **profile_file**: path to a profiles file. (string, default: given by AWS_CREDENTIAL_PROFILES_FILE environment varialbe, or ~/.aws/credentials).

        - **profile_name**: name of a profile. (string, default: `"default"`)

      ```
      [default]
      aws_access_key_id=YOUR_ACCESS_KEY_ID
      aws_secret_access_key=YOUR_SECRET_ACCESS_KEY
  
      [profile2]
      ...
      ```

    - "properties": uses aws.accessKeyId and aws.secretKey Java system properties.

    - "anonymous": uses anonymous access. This auth method can access only public files.

    - "session": uses temporary-generated access_key_id, secret_access_key and session_token.

        - **access_key_id**: AWS access key ID (string, required)

        - **secret_access_key**: AWS secret access key (string, required)

        - **session_token**: session token (string, required)

    - "default": uses AWS SDK's default strategy to look up available credentials from runtime environment. This method behaves like the combination of the following methods.

        1. "env"
        1. "properties"
        1. "profile"
        1. "instance"



### CannedAccessControlList
you can choose one of the below list.

- AuthenticatedRead
- AwsExecRead
- BucketOwnerFullControl
- BucketOwnerRead
- LogDeliveryWrite
- Private
- PublicRead
- PublicReadWrite

cf. http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/CannedAccessControlList.html

## Example

```yaml
path_prefix: logs/out
file_ext: .csv
bucket: my-s3-bucket
endpoint: s3-us-west-1.amazonaws.com
access_key_id: ABCXYZ123ABCXYZ123
secret_access_key: AbCxYz123aBcXyZ123
formatter:
  type: csv
```


## Build

```
$ ./gradlew gem
```
