package org.axesoft.tans.server;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ServiceManager;
import org.apache.commons.cli.*;
import org.axesoft.jaxos.JaxosService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TansServerApp {
    private TansConfig config;
    private JaxosService jaxosService;
    private HttpApiService httpApiService;
    private TansService tansService;
    private ServiceManager serviceManager;
    private PartedThreadPool requestThreadPool;

    private TansServerApp(TansConfig config) {
        this.config = config;
        this.tansService = new TansService(config, () -> this.jaxosService);
        this.requestThreadPool = new PartedThreadPool(this.config.jaxConfig().partitionNumber(), "Request Process Thread");
        this.jaxosService = new JaxosService(config.jaxConfig(), tansService, this.requestThreadPool);
        this.httpApiService = new HttpApiService(config, tansService, this.requestThreadPool);
        this.serviceManager = new ServiceManager(ImmutableList.of(this.jaxosService, this.httpApiService));
    }

    private void start() {
        serviceManager.startAsync();
    }

    public void shutdown() {
        serviceManager.stopAsync();
        try {
            serviceManager.awaitStopped(10, TimeUnit.SECONDS);
        }
        catch (TimeoutException e) {
            e.printStackTrace(System.err);
        }
    }

    public static void main(String[] args) throws Exception {
        TansConfig config = parseArgument(args);

        //For logback config file
        System.setProperty("SERVER_ID", Integer.toString(config.serverId()));
        System.setProperty("LOG_HOME", config.logHome());

        Logger logger = LoggerFactory.getLogger(TansServerApp.class);
        logger.info("Starting TANS server {}", config.serverId());

        TansServerApp app = new TansServerApp(config);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> app.shutdown()));

        app.start();
    }

    private static TansConfig parseArgument(String[] args) throws FileNotFoundException, ParseException {
        Options options = new Options();

        options.addOption("c", "config-file", true, "config file");
        SettingsParser parser = new SettingsParser();
        for (String s : parser.argNames()) {
            if (!"peers".equals(s)) {
                options.addOption(null, s, true, s);
            }
        }

        CommandLineParser cliParser = new DefaultParser();
        CommandLine cli = cliParser.parse(options, args);

        String configFile = getConfigFileName(cli.getOptionValue('c'));

        System.out.println(String.format("Using config file '%s' for TANS ", configFile));

        parser.parse(new FileInputStream(configFile));

        for (String s : parser.argNames()) {
            if (cli.hasOption(s)) {
                parser.parseString(s, cli.getOptionValue(s));
            }
        }

        return new TansConfig(parser.jaxosSettingsBuilder().build(),
                parser.peerHttpPortMap(),
                parser.requestBatchSize(), parser.logHome());
    }

    private static String getConfigFileName(String nameInArgument) {
        if (!Strings.isNullOrEmpty(nameInArgument)) {
            if (new File(nameInArgument).isFile()) {
                return nameInArgument;
            }
            else {
                throw new IllegalArgumentException(nameInArgument + " is not a valid file");
            }
        }

        String[] names = new String[]{System.getProperty("user.home") + "/etc/tans/settings.yml",
                System.getProperty("user.dir") + "/config/settings.yml"};

        for (String fn : names) {
            if (new File(fn).isFile()) {
                return fn;
            }
        }

        throw new IllegalArgumentException("No config file found in these places: " + names[0] + ", " + names[1]);
    }
}
