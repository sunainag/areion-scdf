package com.sproutloud.starter.stream.pool;

import static com.sproutloud.starter.stream.properties.ApplicationConstants.HANDSHAKEMSG1;
import static com.sproutloud.starter.stream.properties.ApplicationConstants.HANDSHAKEMSG2;
import static com.sproutloud.starter.stream.properties.ApplicationConstants.HANDSHAKEMSG3;
import static com.sproutloud.starter.stream.properties.ApplicationConstants.HANDSHAKEMSG4;
import static com.sproutloud.starter.stream.properties.ApplicationConstants.HANDSHAKEMSG5;
import static com.sproutloud.starter.stream.properties.ApplicationConstants.HANDSHAKEMSG6;
import static com.sproutloud.starter.stream.properties.ApplicationConstants.HANDSHAKEMSG7;

import com.sproutloud.starter.stream.properties.AddressCertificationProperties;

import lombok.extern.log4j.Log4j2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Objects;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * Initiates the Satori connection and does satori verification.
 * 
 * @author mgande
 *
 */
@Log4j2
@Component
public class SatoriConnection {

    @Autowired
    private AddressCertificationProperties properties;

    private Socket clientSocket;
    private PrintWriter out;
    private BufferedReader in;

    /**
     * Initiates the connctions and performs initial handshakes.
     */
    @PostConstruct
    public void startConnection() {
        try {
            log.debug("trying to establish connection to satori");
            this.clientSocket = new Socket(InetAddress.getByName(properties.getSatoriIp()), properties.getSatoriPort());
            log.debug("connected to satori +" + clientSocket.isConnected());
            this.out = new PrintWriter(clientSocket.getOutputStream(), true);
            this.in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

            String data = "\t" + "0" + "\t" + properties.getSatoriLicence() + "\n";
            String requestCode = HANDSHAKEMSG1 + String.valueOf(data.length()) + data;
            sendMessage(requestCode, false);
            sendMessage(HANDSHAKEMSG2, false);
            sendMessage(HANDSHAKEMSG3, false);
            sendMessage(HANDSHAKEMSG4, false);
            sendMessage(HANDSHAKEMSG5, false);
            sendMessage(HANDSHAKEMSG6, false);
            sendMessage(HANDSHAKEMSG7, false);

            log.debug("Initial Handshake done");
        } catch (Exception e) {
            log.debug("failed while connecting" + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Sends the messages to Satori and verifies the address.
     * 
     * @param msg address String
     * @param forceReconnect satori has to be force restarted if true.
     * @return certified address String.
     */
    public String sendMessage(String msg, boolean forceReconnect) {
        if (forceReconnect || Objects.isNull(clientSocket) || !clientSocket.isConnected()) {
            log.debug("********* Reconnecting the socket: ******************* \n");
            startConnection();
        }
        log.debug("incoming data: " + msg);
        String resp = null;
        try {
            if (out.checkError()) {
                log.debug("********* Reconnecting the socket as out has errors: ******************* \n");
                startConnection();
            }
            out.write(msg);
            out.flush();
            resp = (String) in.readLine();
            if (!forceReconnect && StringUtils.isEmpty(resp)) {
                log.debug("********* Retrying as response is null for : ******************* \n" + msg);
                resp = sendMessage(msg, true);
            }
            log.debug("response is: " + resp);
        } catch (Exception e) {
            log.debug("inside catch block for data: " + e.getMessage());
        }
        return resp;
    }

    /**
     * Closes the Satori connection.
     */
    @PreDestroy
    public void stopConnection() {
        try {
            in.close();
            out.close();
            if (clientSocket != null && clientSocket.isConnected())
                clientSocket.close();
        } catch (IOException e) {
            log.error("Error stopping connection.");
        }
    }

}