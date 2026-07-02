package src;

import java.net.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Random;

// Receptor GBN sobre UDP - classe única e autocontida (sem dependências externas)
public class Receptor {
    static final int PORTA = 5000;
    static final byte DATA = 0, ACK = 1, HANDSHAKE = 2, FIN = 3, ACK_HS = 4, ACK_FIN = 5;

    static DatagramSocket socket;
    static int expectedseqnum = 0;
    static double perdaSimulada = 0.10;

    static int totalRecebidos = 0;
    static int totalForaDeOrdem = 0;
    static int totalPerdidosSimulados = 0;

    public static void main(String[] args) throws Exception {
        socket = new DatagramSocket(PORTA);
        System.out.println("==================================================");
        System.out.println("      RECEPTOR GBN INICIALIZADO - PORTA " + PORTA);
        System.out.println("==================================================");

        File arquivoSaida = null;
        FileOutputStream out = null;

        // Handshake confiável e idempotente: reconfirma se receber duplicado
        while (out == null) {
            DatagramPacket dp = receber();
            ByteBuffer bb = ByteBuffer.wrap(dp.getData(), 0, dp.getLength());
            byte tipo = bb.get();
            bb.getInt();
            bb.getInt();
            short tam = bb.getShort();
            byte[] dados = new byte[tam];
            bb.get(dados);

            if (tipo != HANDSHAKE) continue;

            String[] partes = new String(dados).split(";");
            String caminhoDestino = partes[0];
            long tamanhoEsperado = Long.parseLong(partes[1]);
            if (partes.length > 2) perdaSimulada = Double.parseDouble(partes[2]);

            System.out.println("[HANDSHAKE] destino=" + caminhoDestino +
                    " tamanho=" + tamanhoEsperado + " perda=" + (perdaSimulada * 100) + "%");

            arquivoSaida = new File(caminhoDestino);
            if (arquivoSaida.getParentFile() != null) arquivoSaida.getParentFile().mkdirs();
            out = new FileOutputStream(arquivoSaida);

            enviar(ACK_HS, 0, 0, null, dp.getAddress(), dp.getPort());
        }

        Random rand = new Random();

        // FSM de recepção do GBN
        while (true) {
            DatagramPacket dp = receber();
            ByteBuffer bb = ByteBuffer.wrap(dp.getData(), 0, dp.getLength());
            byte tipo = bb.get();
            int seq = bb.getInt();
            bb.getInt();
            short tam = bb.getShort();
            byte[] dados = new byte[tam];
            bb.get(dados);
            InetAddress ip = dp.getAddress();
            int porta = dp.getPort();

            if (tipo == HANDSHAKE) {
                enviar(ACK_HS, 0, 0, null, ip, porta); // handshake duplicado: reconfirma
                continue;
            }
            if (tipo == FIN) {
                System.out.println("\n[FIN] Sinal de encerramento recebido.");
                break;
            }

            if (tipo == DATA) {
                if (seq == expectedseqnum) {
                    if (rand.nextDouble() < perdaSimulada) {
                        totalPerdidosSimulados++;
                        System.out.println("[PERDA SIMULADA] seq=" + seq + " descartado propositalmente.");
                        continue; // descarta em silêncio, força timeout no emissor
                    }
                    out.write(dados);
                    totalRecebidos++;
                    System.out.println("[OK] seq=" + seq + " gravado. ACK enviado.");
                    enviar(ACK, 0, expectedseqnum, null, ip, porta);
                    expectedseqnum++;
                } else {
                    totalForaDeOrdem++;
                    System.out.println("[FORA DE ORDEM] seq=" + seq + " esperado=" + expectedseqnum);
                    if (expectedseqnum > 0) enviar(ACK, 0, expectedseqnum - 1, null, ip, porta);
                }
            }
        }

        out.close();
        String hashDestino = sha1(arquivoSaida);

        // Responde a FINs duplicados por até 3s, garantindo que o ACK_FIN chegue
        socket.setSoTimeout(3000);
        long fim = System.currentTimeMillis() + 3000;
        try {
            while (System.currentTimeMillis() < fim) {
                try {
                    DatagramPacket dp = receber();
                    ByteBuffer bb = ByteBuffer.wrap(dp.getData(), 0, dp.getLength());
                    byte tipo = bb.get();
                    if (tipo == FIN) {
                        enviar(ACK_FIN, 0, expectedseqnum - 1, hashDestino.getBytes(), dp.getAddress(), dp.getPort());
                    }
                } catch (SocketTimeoutException ste) {
                    break; // ninguém mais reenviou FIN: pode encerrar
                }
            }
        } finally {
            socket.close();
        }

        int totalEventos = totalRecebidos + totalPerdidosSimulados;
        double taxaEfetiva = totalEventos == 0 ? 0 : (double) totalPerdidosSimulados / totalEventos;

        System.out.println("\n==================================================");
        System.out.println("           ESTATÍSTICAS DO RECEPTOR               ");
        System.out.println("==================================================");
        System.out.println("Pacotes gravados em ordem: " + totalRecebidos);
        System.out.println("Pacotes fora de ordem descartados: " + totalForaDeOrdem);
        System.out.println("Descartes simulados por perda: " + totalPerdidosSimulados);
        System.out.println("Taxa de perda efetiva: " + String.format("%.2f", taxaEfetiva * 100) + "%");
        System.out.println("[SHA-1] Hash do arquivo recebido: " + hashDestino);
        System.out.println("==================================================");
    }

    static DatagramPacket receber() throws Exception {
        byte[] buffer = new byte[1500];
        DatagramPacket dp = new DatagramPacket(buffer, buffer.length);
        socket.receive(dp);
        return dp;
    }

    // Monta e envia um datagrama de controle (cabeçalho de 11 bytes + payload)
    static void enviar(byte tipo, int seq, int ack, byte[] dados, InetAddress ip, int porta) throws Exception {
        byte[] d = dados == null ? new byte[0] : dados;
        ByteBuffer buf = ByteBuffer.allocate(11 + d.length);
        buf.put(tipo);
        buf.putInt(seq);
        buf.putInt(ack);
        buf.putShort((short) d.length);
        buf.put(d);
        byte[] b = buf.array();
        DatagramPacket dp = new DatagramPacket(b, b.length, ip, porta);
        socket.send(dp);
    }

    static String sha1(File f) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        try (FileInputStream in = new FileInputStream(f)) {
            byte[] buffer = new byte[4096];
            int n;
            while ((n = in.read(buffer)) != -1) md.update(buffer, 0, n);
        }
        StringBuilder sb = new StringBuilder();
        for (byte b : md.digest()) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}