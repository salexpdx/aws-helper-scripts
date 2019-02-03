#!/usr/local/bin/python3.6
# Python script that pulls AMIs in account/region and uses information from them
# to add tags to snapshots to help determine which ones are worth retaining.
# Original source / updates at https://github.com/salexpdx/aws-helper-scripts

#     This program is free software: you can redistribute it and/or modify
#     it under the terms of the GNU General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.
#
#     This program is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.
#
#     You should have received a copy of the GNU General Public License
#     along with this program.  If not, see <https://www.gnu.org/licenses/>.

import boto3
from botocore.exceptions import ClientError
import pprint
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('add_tags_to_snapshot.py')
#logger.setLevel(logging.DEBUG)

ec2client = boto3.client("ec2")

# First, let's loop through all snapshots and for any that don't have a tag called Name, see if the instance
# that the snapshot was created from still exists.  If it does, we will grab the Name tag from that instance
# and put it into the snapshot description.

# let's get our account ID since we only want to look at snapshots we own.
account_id = boto3.client("sts").get_caller_identity().get("Account")
logger.info(f'currently running in account {account_id}')
snapshots_response = ec2client.describe_snapshots(OwnerIds=[account_id])
snapshots = snapshots_response["Snapshots"]

for snapshot in snapshots:
    logger.debug(f'found snapshot with details {snapshot}')
    has_name_tag = False
    if "Tags" in snapshot:
        for tag in snapshot["Tags"]:
            if tag["Key"] == 'Name':
                has_name_tag = True

    description = snapshot["Description"]
    if description.startswith("Created by CreateImage"):
        instance_id = description[23:].split(")")[0]
        # See if the instance is still running
        try:
            instances_response = ec2client.describe_instances(InstanceIds=[instance_id])
            # we provided an instance id, if the call succeeded, there is only one instance in the list
            instance = instances_response["Reservations"][0]["Instances"][0]
            if "Tags" in instance:
                for tag in instance["Tags"]:
                    if tag["Key"] == 'Name':
                        instance_name = tag["Value"]
                        logger.info(f'Found Name tag with value {instance_name}')
                        tag_key = "Name"
                        if has_name_tag:
                            tag_key = "instance_name"
                        # need to have a snapshot resource to create tags on the snapshot.
                        snapshot_resource = boto3.resource('ec2').Snapshot(snapshot["SnapshotId"])
                        logger.warning(f'Adding tag to snapshot {snapshot["SnapshotId"]} {tag_key} = {tag["Value"]}')
                        snapshot_resource.create_tags(
                            Tags=[
                                {
                                    'Key': tag_key,
                                    'Value': tag["Value"]
                                }
                            ]
                        )
            else:
                logger.debug(f'No tags found for instance {instance_id}.')

        except ClientError as e:
            logger.debug(f'Snapshot {snapshot["SnapshotId"]} came from {instance_id} which no longer exists')
            logger.debug(e)

        except Exception as e:
            logging.warning(f'Unknown error occurred looking up {instance_id}')
            logging.warning(e)
            exit(1)



# Next we are going to loop through every AMI that we own and use information from that to tag the snapshot
image_response = ec2client.describe_images(Owners=["self"])
images = image_response["Images"]

logger = logging.getLogger()
for image in images:
    logger.debug(f'Processing {image["ImageId"]}')
    block_device_mappings = image["BlockDeviceMappings"]
    for mapping in block_device_mappings:
        if "Ebs" in mapping and "SnapshotId" in mapping["Ebs"]:
            logger.debug(f'{mapping}')
            snapshot = mapping["Ebs"]["SnapshotId"]
            image_id = image["ImageId"]
            description = image["Description"]
            image_location = image["ImageLocation"]
            logger.debug(f'Image {image_id} uses snapshot {snapshot} with image location {image_location} and description {description}')

            # create a list of tags we are going to apply.  Doing this here so we can add name later if the snapshot
            # doesn't already have a "Name" tag.
            new_tags = [
                {
                    'Key': "ami_image_id",
                    'Value': image_id
                },
                {
                    'Key': "ami_description",
                    'Value': description
                },
                {
                    'Key': "ami_image_location",
                    'Value': image_location
                }
            ]

            snapshot = boto3.resource("ec2").Snapshot(snapshot)

            has_name_tag = False
            if not isinstance(snapshot.tags, type(None)):
                for tag in snapshot.tags:
                    if tag["Key"] == 'Name':
                        has_name_tag = True

            if not has_name_tag:
                logger.info("No name tag found on resource, setting Name tag")
                if len(description) > 0:
                    new_tags.append(
                        {
                            'Key': "Name",
                            'Value': f'copied from AMI: {description}'
                        }
                    )
                else:
                    new_tags.append(
                        {
                            'Key': "Name",
                            'Value': f'copied from AMI: {image_location}'
                        }
                    )

            # now tag the snapshot with details about the AMI
            logger.debug(f'Tagging snapshot {new_tags}')

            snapshot.create_tags(
                Tags=new_tags
            )



