import sqlite3
from rosidl_runtime_py.utilities import get_message
from rclpy.serialization import deserialize_message
import pandas as pd
import os

# Ensure directory exists
def ensure_directory_exists(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

# Convert nested ROS2 message to a flat dictionary
def flatten_ros_message(msg):
    flat_dict = {}

    def _flatten(field_name, value):
        if hasattr(value, 'get_fields_and_field_types'):  # Nested ROS message
            sub_fields = value.get_fields_and_field_types().keys()
            for sub_field in sub_fields:
                sub_value = getattr(value, sub_field)
                _flatten(f"{field_name}.{sub_field}" if field_name else sub_field, sub_value)
        elif isinstance(value, (list, tuple)):  # Lists, like covariance
            for i, item in enumerate(value):
                _flatten(f"{field_name}[{i}]", item)
        else:
            flat_dict[field_name] = value

    _flatten('', msg)
    return flat_dict

# Write data to CSV
def write_to_csv(file_path, timestamps, parsed_data):
    ensure_directory_exists(file_path)
    df = pd.DataFrame(parsed_data)
    df.insert(0, 'timestamp', timestamps)
    df.to_csv(file_path, index=False)

# Extract data from bag file for a given topic
def extract_data_from_bag(bag_file, topic_name, csv_file_path):
    # Connect to the database
    conn = sqlite3.connect(bag_file)
    c = conn.cursor()

    # Get the topic ID and message type
    c.execute('SELECT id, type FROM topics WHERE name=?', (topic_name,))
    topic_info = c.fetchone()
    
    if not topic_info:
        print(f"Topic {topic_name} not found.")
        return
    
    topic_id, msg_type = topic_info
    msg_class = get_message(msg_type)

    # Fetch all messages for the topic
    c.execute('SELECT timestamp, data FROM messages WHERE topic_id=?', (topic_id,))
    messages = c.fetchall()

    # Deserialize and parse messages
    timestamps = []
    parsed_data = []
    for timestamp, data in messages:
        msg = deserialize_message(data, msg_class)
        msg_dict = flatten_ros_message(msg)
        timestamps.append(timestamp)
        parsed_data.append(msg_dict)

    # Write to CSV
    write_to_csv(csv_file_path, timestamps, parsed_data)

    # Close the connection
    conn.close()

if __name__ == "__main__":
    bag_file = '/home/nisarg/testes/7_test/7_test_0.db3'
    output_dir = '/home/nisarg/testes/extracted_data/7_test'

    # Define the topics to extract and corresponding output CSV files
    topics_to_extract = {
        '/odometry':            f'{output_dir}/odometry.csv',
        '/gps/fix':             f'{output_dir}/gps_fix.csv',
        '/gps/navpvt':          f'{output_dir}/gps_navpvt.csv',
        '/odometry/global':     f'{output_dir}/odometry_global.csv',
        '/odometry/gps':        f'{output_dir}/odometry_gps.csv',
        '/odometry/local':      f'{output_dir}/odometry_local.csv',
        '/plan' :               f'{output_dir}/planned_path.csv',
        '/imu':                 f'{output_dir}/imu.csv',
        '/imu/calib_status':    f'{output_dir}/imu_calib.csv',
        '/cmd_vel':             f'{output_dir}/cmd_vel.csv',
    }

    for topic, csv_file in topics_to_extract.items():
        print(f"Extracting data for topic: {topic}")
        extract_data_from_bag(bag_file, topic, csv_file)
        print(f"Data for {topic} saved to {csv_file}")
