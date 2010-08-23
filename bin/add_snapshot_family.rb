#
# Copyright 2010 The Apache Software Foundation
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Script adds the snapshot column family to .META. table
# if it does not exist. This is used if the table data
# is migrated from an older hbase version.
#
# To see usage for this script, run:
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main add_snapshot_family.rb --help
#

include Java
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.io.hfile.Compression
import org.apache.hadoop.hbase.regionserver.StoreFile
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.MetaUtils
import org.apache.hadoop.hbase.util.MetaUtils.ScannerListener
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor

# Name of this script
NAME = 'add_snapshot_family'

# Print usage for this script
def usage
  puts 'Usage: %s.rb' % NAME
  puts 'Script adds the snapshot column family to .META. table if it does not exist.'
  exit!
end

# Snapshot column family descriptor
snapshotHcd = HColumnDescriptor.new(HConstants::SNAPSHOT_FAMILY, 10,
				Compression::Algorithm::NONE.getName(), true, true, 8 * 1024,
				HConstants::FOREVER, StoreFile::BloomType::NONE.toString(),
				HConstants::REPLICATION_SCOPE_LOCAL)

# Get configuration to use.
c = HBaseConfiguration.new()

# Get a logger and a metautils instance.
LOG = LogFactory.getLog(NAME)
utils = MetaUtils.new(c)

# Start
begin
	class RootScannerListener
	  include org.apache.hadoop.hbase.util.MetaUtils.ScannerListener

		# Process each region's info of META table.
		def processRow(info)
			if Bytes.equals(info.getTableDesc().getName(), HConstants::META_TABLE_NAME)
				families = info.getTableDesc().getColumnFamilies()
				for f in families
					if Bytes.equals(f.getName(), HConstants::SNAPSHOT_FAMILY)
						# META table already has SNAPSHOT family
						# return false to terminate the scan.
						LOG.info("Snapshot family already exists for .META. table")
						return false
					end
				end
				# Add snapshot family otherwise.
			  info.getTableDesc().addFamily(snapshotHcd)
			  utils.updateMETARegionInfo(utils.getRootRegion(), info)
			end
			return true
		end
	end

	utils.scanRootRegion(RootScannerListener.new())
ensure
  utils.shutdown()
end
