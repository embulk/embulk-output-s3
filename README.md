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
- **tmp_path_prefix**: prefix of temporary files (string, default: 'embulk-output-s3-')
- **canned_acl**: canned access control list for created objects ([enum](#cannedaccesscontrollist), default: null)

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
