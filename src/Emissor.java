package src;

import java.net.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;

// Emissor GBN sobre UDP - classe única e autocontida (sem dependências externas)
public class Emissor {
    static final int PORTA_RECEPTOR = 5000;
    static final byte DATA = 0, ACK = 1, HANDSHAKE = 2, FIN = 3, ACK_HS = 4, ACK_FIN = 5;
    static final long TIMEOUT = 1000; // 1 segundo

    static DatagramSocket socket;
    static InetAddress destino;
    static int windowSize;
    static double perda;

    static int base = 0;
    static int nextseqnum = 0;
    static final HashMap<Integer, byte[]> buffer = new HashMap<>(); // pacotes não confirmados
    static long timerInicio;
    static boolean timerAtivo = false;

    static volatile boolean arquivoLidoComSucesso = false;
    static volatile boolean terminou = false;

    static int totalRetransmissoes = 0;
    static int totalPacotesEnviados = 0;

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.out.println("Uso: java src.Emissor <arquivo_origem> <IP_destino>:<path_destino> <tamanho_janela> <prob_perda>");
            return;
        }

        String arquivoOrigem = args[0];
        windowSize = Integer.parseInt(args[2]);
        perda = Double.parseDouble(args[3]);

        String[] partes = args[1].split(":", 2);
        if (partes.length < 2) {
            System.out.println("Erro: destino deve ser <IP>:<path>");
            return;
        }
        destino = InetAddress.getByName(partes[0]);
        String pathDestino = partes[1];

        socket = new DatagramSocket();

        File fOrigem = new File(arquivoOrigem);
        if (!fOrigem.exists()) {
            System.out.println("Erro: arquivo de origem não encontrado!");
            return;
        }

        String hashOrigem = sha1(fOrigem);
        System.out.println("[SHA-1] Hash do arquivo de origem: " + hashOrigem);

        if (!handshakeConfiavel(pathDestino, fOrigem.length())) {
            System.out.println("Erro: receptor não respondeu ao handshake após várias tentativas.");
            socket.close();
            return;
        }

        long tempoInicial = System.currentTimeMillis();

        Thread watchdog = new Thread(Emissor::watchdog);
        watchdog.setDaemon(true);
        watchdog.start();

        Thread envio = new Thread(() -> enviarArquivo(arquivoOrigem));
        Thread ack = new Thread(Emissor::receberACK);
        envio.start();
        ack.start();
        envio.join();
        ack.join();

        double segundos = (System.currentTimeMillis() - tempoInicial) / 1000.0;
        double throughput = (fOrigem.length() / 1024.0) / (segundos == 0 ? 1 : segundos);

        String hashDestino = finConfiavel();

        System.out.println("\n==================================================");
        System.out.println("           TRANSFERÊNCIA FINALIZADA               ");
        System.out.println("==================================================");
        System.out.println("Pacotes DATA originais enviados: " + nextseqnum);
        System.out.println("Datagramas totais (DATA+controle+retrans.): " + totalPacotesEnviados);
        System.out.println("Retransmissões por timeout: " + totalRetransmissoes);
        System.out.println("Tempo de execução: " + segundos + " s");
        System.out.println("Throughput estimado: " + String.format("%.2f", throughput) + " KB/s");
        System.out.println("--------------------------------------------------");
        System.out.println("[SHA-1] Origem : " + hashOrigem);
        System.out.println("[SHA-1] Destino: " + (hashDestino != null ? hashDestino : "(não confirmado)"));
        if (hashDestino != null) {
            System.out.println("[INTEGRIDADE] " + (hashOrigem.equals(hashDestino) ? "IDÊNTICOS - transferência íntegra." : "DIFERENTES - possível corrupção!"));
        }
        System.out.println("==================================================");

        socket.close();
    }

    // Watchdog do timer único da janela (base)
    static void watchdog() {
        try {
            while (!terminou) {
                Thread.sleep(10);
                synchronized (Emissor.class) {
                    if (timerAtivo && System.currentTimeMillis() - timerInicio >= TIMEOUT) {
                        System.out.println("\n[TIMEOUT] Retransmitindo janela a partir do pacote: " + base);
                        timerInicio = System.currentTimeMillis();
                        for (int i = base; i < nextseqnum; i++) {
                            byte[] dados = buffer.get(i);
                            if (dados != null) { enviarControle(DATA, i, 0, dados); totalRetransmissoes++; }
                        }
                    }
                }
            }
        } catch (Exception e) { if (!terminou) e.printStackTrace(); }
    }

    // Lê o arquivo e despacha pacotes respeitando a janela deslizante
    static void enviarArquivo(String arquivo) {
        try (FileInputStream in = new FileInputStream(arquivo)) {
            byte[] dados = new byte[1024];
            int lido;
            while ((lido = in.read(dados)) != -1) {
                synchronized (Emissor.class) {
                    while (nextseqnum >= base + windowSize) Emissor.class.wait();
                    byte[] bloco = Arrays.copyOf(dados, lido);
                    buffer.put(nextseqnum, bloco);
                    enviarControle(DATA, nextseqnum, 0, bloco);
                    System.out.println("[ENVIO] seq=" + nextseqnum + " (janela " + base + " a " + (base + windowSize - 1) + ")");
                    if (base == nextseqnum) { timerInicio = System.currentTimeMillis(); timerAtivo = true; }
                    nextseqnum++;
                }
            }
            synchronized (Emissor.class) {
                arquivoLidoComSucesso = true;
                while (base != nextseqnum) Emissor.class.wait();
            }
        } catch (Exception e) { e.printStackTrace(); }
    }

    // Processa ACKs recebidos e avança a base da janela
    static void receberACK() {
        try {
            byte[] buf = new byte[1500];
            while (!terminou) {
                DatagramPacket dp = new DatagramPacket(buf, buf.length);
                try { socket.receive(dp); } catch (SocketException se) { break; }

                ByteBuffer bb = ByteBuffer.wrap(dp.getData(), 0, dp.getLength());
                byte tipo = bb.get();
                bb.getInt();
                int ack = bb.getInt();

                if (tipo == ACK) {
                    synchronized (Emissor.class) {
                        System.out.println("   [ACK] confirmado até: " + ack);
                        if (ack >= base) {
                            base = ack + 1;
                            buffer.keySet().removeIf(k -> k <= ack);
                            if (base == nextseqnum) {
                                timerAtivo = false;
                                if (arquivoLidoComSucesso) terminou = true;
                            } else { timerInicio = System.currentTimeMillis(); timerAtivo = true; }
                            Emissor.class.notifyAll();
                        }
                    }
                }
            }
        } catch (Exception e) { e.printStackTrace(); }
    }

    // Handshake com retransmissão até receber ACK_HS (até 5 tentativas)
    static boolean handshakeConfiavel(String path, long tamanho) throws Exception {
        byte[] dados = (path + ";" + tamanho + ";" + perda).getBytes();
        socket.setSoTimeout(1000);
        for (int t = 1; t <= 5; t++) {
            enviarControle(HANDSHAKE, 0, 0, dados);
            try {
                byte[] buf = new byte[1500];
                DatagramPacket dp = new DatagramPacket(buf, buf.length);
                socket.receive(dp);
                if (dp.getData()[0] == ACK_HS) { socket.setSoTimeout(0); return true; }
            } catch (SocketTimeoutException e) { System.out.println("[HANDSHAKE] sem resposta, tentativa " + t + "/5..."); }
        }
        socket.setSoTimeout(0);
        return false;
    }

    // FIN com retransmissão até receber ACK_FIN (que traz o hash do receptor)
    static String finConfiavel() {
        try {
            System.out.println("[FIN] Enviando sinalizador de término...");
            socket.setSoTimeout(1000);
            for (int t = 1; t <= 5; t++) {
                enviarControle(FIN, nextseqnum, 0, null);
                try {
                    byte[] buf = new byte[1500];
                    DatagramPacket dp = new DatagramPacket(buf, buf.length);
                    socket.receive(dp);
                    ByteBuffer bb = ByteBuffer.wrap(dp.getData(), 0, dp.getLength());
                    byte tipo = bb.get();
                    bb.getInt();
                    bb.getInt();
                    short tam = bb.getShort();
                    if (tipo == ACK_FIN) { byte[] hash = new byte[tam]; bb.get(hash); return new String(hash); }
                } catch (SocketTimeoutException e) { System.out.println("[FIN] sem confirmação, retransmitindo (" + t + "/5)..."); }
            }
        } catch (Exception e) { e.printStackTrace(); }
        return null;
    }

    // Monta e envia um datagrama de aplicação (cabeçalho de 11 bytes + payload)
    static void enviarControle(byte tipo, int seq, int ack, byte[] dados) throws Exception {
        byte[] d = dados == null ? new byte[0] : dados;
        ByteBuffer buf = ByteBuffer.allocate(11 + d.length);
        buf.put(tipo); buf.putInt(seq); buf.putInt(ack); buf.putShort((short) d.length); buf.put(d);
        byte[] b = buf.array();
        DatagramPacket dp = new DatagramPacket(b, b.length, destino, PORTA_RECEPTOR);
        socket.send(dp);
        totalPacotesEnviados++;
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