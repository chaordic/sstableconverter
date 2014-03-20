/*
 * Licensed to the Chaordic Systems under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.chaordicsystems.sstableconverter;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;
import org.apache.cassandra.tools.BulkLoader.CmdLineOptions;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

@SuppressWarnings("rawtypes")
public class SSTableConverter
{
    static
    {
        Config.setLoadYaml(false);
    }

    public static void main(String[] args) throws Exception
    {
        LoaderOptions options = LoaderOptions.parseArgs(args);
        OutputHandler handler = new OutputHandler.SystemOutput(options.verbose, options.debug);

        File srcDir = options.sourceDir;
        File dstDir = options.destDir;
        IPartitioner srcPart = options.srcPartitioner;
        IPartitioner dstPart = options.dstPartitioner;
        String keyspace = options.ks;
        String cf = options.cf;

        if (keyspace == null) {
            keyspace = srcDir.getParentFile().getName();
        }
        if (cf == null) {
            cf = srcDir.getName();
        }

        CFMetaData metadata = new CFMetaData(keyspace, cf, ColumnFamilyType.Standard, BytesType.instance, null);
        Collection<SSTableReader> originalSstables = readSSTables(srcPart, srcDir, metadata, handler);

        handler.output(String.format("Converting sstables of ks[%s], cf[%s], from %s to %s. Src dir: %s. Dest dir: %s.",
                                     keyspace, cf, srcPart.getClass().getName(), dstPart.getClass().getName(),
                                     srcDir.getAbsolutePath(), dstDir.getAbsolutePath()));

        SSTableSimpleUnsortedWriter destWriter = new SSTableSimpleUnsortedWriter(dstDir,
                                    dstPart, keyspace, cf, AsciiType.instance, null, 64);

        for (SSTableReader reader : originalSstables) {
            handler.output("Reading: " + reader.getFilename());
            SSTableIdentityIterator row;
            SSTableScanner scanner = reader.getDirectScanner(null);

            // collecting keys to export
            while (scanner.hasNext())
            {
                row = (SSTableIdentityIterator) scanner.next();

                destWriter.newRow(row.getKey().key);
                while (row.hasNext()) {
                    IColumn col = (IColumn)row.next();
                    destWriter.addColumn(col.name(), col.value(), col.timestamp());
                }
            }
            scanner.close();
        }

        // Don't forget to close!
        destWriter.close();

        System.exit(0);
    }

    public static Collection<SSTableReader> readSSTables(final IPartitioner srcPartitioner, File srcDir,
                                                         final CFMetaData metadata,
                                                         final OutputHandler outputHandler)
    {
        final List<SSTableReader> sstables = new ArrayList<SSTableReader>();
        srcDir.list(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                if (new File(dir, name).isDirectory())
                    return false;
                Pair<Descriptor, Component> p = SSTable.tryComponentFromFilename(dir, name);
                Descriptor desc = p == null ? null : p.left;
                if (p == null || !p.right.equals(Component.DATA) || desc.temporary)
                    return false;

                if (!new File(desc.filenameFor(Component.PRIMARY_INDEX)).exists())
                {
                    outputHandler.output(String.format("Skipping file %s because index is missing", name));
                    return false;
                }

                Set<Component> components = new HashSet<Component>();
                components.add(Component.DATA);
                components.add(Component.PRIMARY_INDEX);
                if (new File(desc.filenameFor(Component.COMPRESSION_INFO)).exists())
                    components.add(Component.COMPRESSION_INFO);
                if (new File(desc.filenameFor(Component.STATS)).exists())
                    components.add(Component.STATS);

                try
                {
                    sstables.add(SSTableReader.openForBatch(desc, components, srcPartitioner, metadata));
                }
                catch (IOException e)
                {
                    outputHandler.output(String.format("Skipping file %s, error opening it: %s", name, e.getMessage()));
                }
                return false;
            }
        });
        return sstables;
    }

    static class LoaderOptions
    {
        private static final String TOOL_NAME = "sstableconverter";
        private static final String VERBOSE_OPTION  = "verbose";
        private static final String DEBUG_OPTION  = "debug";
        private static final String HELP_OPTION  = "help";
        private static final String CF_OPTION = "cf";
        private static final String KS_OPTION = "ks";
        private static final String SOURCE_PARTITIONER_OPTION = "source-partitioner";
        private static final String DEST_PARTITIONER_OPTION = "dest-partitioner";
        private static final String PARTITIONER_PKG_PREFIX = "org.apache.cassandra.dht.";
        private static final String DEFAULT_DST_PARTITIONER = "Murmur3Partitioner";
        private static final String DEFAULT_SRC_PARTITIONER = "RandomPartitioner";
        public final File sourceDir;
        public final File destDir;
        public final String cf;
        public final String ks;
        public final IPartitioner srcPartitioner;
        public final IPartitioner dstPartitioner;

        public boolean debug;
        public boolean verbose;
        public String sourcePath;

        LoaderOptions(File sourceDir, File destDir, IPartitioner srcPartitioner, IPartitioner dstPartitioner,
                      String ks, String cf)
        {
            this.sourceDir = sourceDir;
            this.destDir = destDir;
            this.srcPartitioner = srcPartitioner;
            this.dstPartitioner = dstPartitioner;
            this.ks = ks;
            this.cf = cf;
        }

        public static LoaderOptions parseArgs(String cmdArgs[])
        {
            CommandLineParser parser = new GnuParser();
            CmdLineOptions options = getCmdLineOptions();
            try
            {
                CommandLine cmd = parser.parse(options, cmdArgs, false);

                if (cmd.hasOption(HELP_OPTION))
                {
                    printUsage(options);
                    System.exit(0);
                }

                String[] args = cmd.getArgs();
                if (args.length < 2)
                {
                    System.err.println("Not enough arguments");
                    printUsage(options);
                    System.exit(1);
                }

                if (args.length > 2)
                {
                    System.err.println("Too many arguments");
                    printUsage(options);
                    System.exit(1);
                }

                String srcDir = args[0];
                File sourceDir = new File(srcDir);
                if (!sourceDir.exists())
                    errorMsg("Unknown directory: " + srcDir, options);
                if (!sourceDir.isDirectory())
                    errorMsg(srcDir + " is not a directory", options);

                String dstDir = args[1];
                File destDir = new File(dstDir);
                if (!destDir.exists()){
                    System.out.println("Directory " + destDir + " does not existing. Creating it.");
                    destDir.mkdirs();
                } else if (!destDir.isDirectory()) {
                    errorMsg(destDir + " is not a directory", options);
                }

                String srcPartitioner = PARTITIONER_PKG_PREFIX + DEFAULT_SRC_PARTITIONER;
                String dstPartitioner = PARTITIONER_PKG_PREFIX + DEFAULT_DST_PARTITIONER;

                if (cmd.hasOption(SOURCE_PARTITIONER_OPTION))
                    srcPartitioner = cmd.getOptionValue(SOURCE_PARTITIONER_OPTION);
                if (cmd.hasOption(DEST_PARTITIONER_OPTION))
                    dstPartitioner = cmd.getOptionValue(DEST_PARTITIONER_OPTION);

                IPartitioner sourcePartitioner = null;
                try {
                    sourcePartitioner = (IPartitioner) Class.forName(srcPartitioner).newInstance();
                } catch (Exception e) {
                    errorMsg("Source partitioner " + srcPartitioner + " does not exist.", options);
                }
                IPartitioner destPartitioner = null;
                try {
                    destPartitioner = (IPartitioner) Class.forName(dstPartitioner).newInstance();
                } catch (Exception e) {
                    errorMsg("Destination partitioner " + dstPartitioner + " doest not exist.", options);
                }

                String ks = null;
                if (cmd.hasOption(KS_OPTION))
                    ks = cmd.getOptionValue(KS_OPTION);
                String cf = null;
                if (cmd.hasOption(CF_OPTION))
                    cf = cmd.getOptionValue(CF_OPTION);

                LoaderOptions opts = new LoaderOptions(sourceDir, destDir, sourcePartitioner, destPartitioner, ks, cf);
                opts.debug = cmd.hasOption(DEBUG_OPTION);
                opts.verbose = cmd.hasOption(VERBOSE_OPTION);

                return opts;
            }
            catch (ParseException e)
            {
                errorMsg(e.getMessage(), options);
                return null;
            }
        }

        private static void errorMsg(String msg, CmdLineOptions options)
        {
            System.err.println(msg);
            printUsage(options);
            System.exit(1);
        }
        private static CmdLineOptions getCmdLineOptions()
        {
            CmdLineOptions options = new CmdLineOptions();
            options.addOption(null, DEBUG_OPTION,        "display stack traces");
            options.addOption("v",  VERBOSE_OPTION,      "verbose output");
            options.addOption("h",  HELP_OPTION,         "display this help message");
            options.addOption("c",  CF_OPTION, "columnfamily",         "Column family of <src_dir> (default: <src_dir> name)");
            options.addOption("k",  KS_OPTION, "keyspace",            "Keyspace of <src_dir> (default: <src_dir>/../ name)");
            options.addOption("s",  SOURCE_PARTITIONER_OPTION,
                    "sets the source partitioner (default: " + DEFAULT_SRC_PARTITIONER + ")");
            options.addOption("d",  DEST_PARTITIONER_OPTION,
                    "sets the destination partitioner (default: " + DEFAULT_DST_PARTITIONER + ")");
            return options;
        }

        public static void printUsage(Options options)
        {
            String usage = String.format("%s [options] <src_dir> <dst_dir>", TOOL_NAME);
            StringBuilder header = new StringBuilder();
            header.append("--\n");
            header.append("Converts the sstables found in the directory <src_dir> ");
            header.append("from a source partitioner (specified by -s option)");
            header.append("to a destination partitioner (specified -d option), ");
            header.append("storing the converted sstables on <dst_dir>.\n");
            header.append("If the keyspace and cf options are not provided, <src_dir> is used as");
            header.append("cf name and the parent directory of <dir_path> is used as the keyspace name.");
            header.append("\n--\n");
            header.append("Options are:");
            new HelpFormatter().printHelp(usage, header.toString(), options, "");
        }
    }
}