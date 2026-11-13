package org.owlang.night.mail;

import jakarta.mail.Authenticator;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import org.owlang.night.data.NightJdbc;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;

/**
 * Text-only authenticated SMTP sender with required STARTTLS or SMTPS,
 * bounded templates and JDBC audit state. A successful SMTP handoff is not
 * proof of inbox delivery; the audit distinguishes acceptance from delivery.
 */
public final class NightMail {
    private final NightJdbc jdbc;
    private final Clock clock;

    public NightMail(NightJdbc jdbc) {
        this(jdbc, Clock.systemUTC());
    }

    NightMail(NightJdbc jdbc, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
    }

    private static String required(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank())
            throw new IllegalStateException("Night Mail requires " + name);
        return value;
    }

    static String address(String raw) {
        if (raw == null || raw.length() > 254 || raw.indexOf('\r') >= 0
                || raw.indexOf('\n') >= 0 || raw.indexOf('<') >= 0 || raw.indexOf('>') >= 0)
            throw new IllegalArgumentException("Invalid recipient address");
        try {
            InternetAddress parsed = new InternetAddress(raw, true);
            parsed.validate();
            if (!raw.equals(parsed.getAddress()) || raw.indexOf('@') < 1)
                throw new IllegalArgumentException("Invalid recipient address");
            return raw;
        } catch (jakarta.mail.internet.AddressException ex) {
            throw new IllegalArgumentException("Invalid recipient address", ex);
        }
    }

    static String template(String text, List<?> keys, List<?> values) {
        if (text == null || text.length() > 64_000 || keys == null || values == null
                || keys.size() != values.size() || keys.size() > 20)
            throw new IllegalArgumentException("Invalid Night Mail template");
        String result = text;
        for (int i = 0; i < keys.size(); i++) {
            if (!(keys.get(i) instanceof String name)
                    || !name.matches("[A-Za-z_][A-Za-z0-9_]{0,31}")
                    || !(values.get(i) instanceof String value) || value.length() > 8_192)
                throw new IllegalArgumentException("Invalid Night Mail variable");
            result = result.replace("{{" + name + "}}", value);
        }
        if (result.contains("{{") || result.contains("}}") || result.length() > 64_000)
            throw new IllegalArgumentException("Unresolved or oversized Night Mail template");
        return result;
    }

    public int send(String eventId, String recipient, String subject,
            String bodyTemplate, List<?> keys, List<?> values) {
        if (eventId == null || !eventId.matches("[A-Za-z0-9_-]{8,80}"))
            throw new IllegalArgumentException("Invalid mail event ID");
        String to = address(recipient);
        if (subject == null || subject.isBlank() || subject.length() > 160
                || subject.indexOf('\r') >= 0 || subject.indexOf('\n') >= 0)
            throw new IllegalArgumentException("Invalid mail subject");
        String body = template(bodyTemplate, keys, values);
        String host = required("NIGHT_SMTP_HOST");
        String user = required("NIGHT_SMTP_USERNAME");
        String password = required("NIGHT_SMTP_PASSWORD");
        String from = address(required("NIGHT_SMTP_FROM"));
        int port;
        try {
            port = Integer.parseInt(System.getenv().getOrDefault("NIGHT_SMTP_PORT", "587"));
        } catch (NumberFormatException ex) {
            throw new IllegalStateException("Invalid NIGHT_SMTP_PORT", ex);
        }
        if (port != 587 && port != 465)
            throw new IllegalStateException("Only SMTP STARTTLS/587 or SMTPS/465 is supported");
        if (!host.matches("[A-Za-z0-9][A-Za-z0-9.-]{0,251}"))
            throw new IllegalStateException("Invalid SMTP hostname");

        // Inserting the audit row first is an idempotency fence for event IDs.
        // SMTP acceptance and audit update are not atomic: failure/retry needs
        // operator reconciliation rather than an automatic blind resend.
        jdbc.execute("""
                INSERT INTO night_mail_audit (event_id, recipient_sha256, state, created_at, updated_at)
                VALUES (?, ?, 'PENDING', ?, ?)
                """, List.of(eventId, sha256(to.toLowerCase(java.util.Locale.ROOT)),
                        clock.millis(), clock.millis()));

        Properties p = new Properties();
        p.put("mail.smtp.host", host);
        p.put("mail.smtp.port", Integer.toString(port));
        p.put("mail.smtp.auth", "true");
        p.put("mail.smtp.connectiontimeout", "5000");
        p.put("mail.smtp.timeout", "10000");
        p.put("mail.smtp.writetimeout", "10000");
        p.put("mail.smtp.ssl.checkserveridentity", "true");
        p.put("mail.smtp.starttls.enable", port == 587 ? "true" : "false");
        p.put("mail.smtp.starttls.required", port == 587 ? "true" : "false");
        p.put("mail.smtp.ssl.enable", port == 465 ? "true" : "false");

        Session session = Session.getInstance(p, new Authenticator() {
            @Override protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(user, password);
            }
        });

        try {
            MimeMessage msg = new MimeMessage(session);
            msg.setFrom(new InternetAddress(from));
            msg.setRecipient(Message.RecipientType.TO, new InternetAddress(to));
            msg.setSubject(subject, "UTF-8");
            msg.setText(body, "UTF-8");
            Transport.send(msg);
            jdbc.execute("UPDATE night_mail_audit SET state = 'ACCEPTED', updated_at = ? WHERE event_id = ?",
                    List.of(clock.millis(), eventId));
            return 1;
        } catch (MessagingException ex) {
            jdbc.execute("UPDATE night_mail_audit SET state = 'FAILED', updated_at = ? WHERE event_id = ?",
                    List.of(clock.millis(), eventId));
            throw new IllegalStateException("Night SMTP handoff failed; see mail audit status", ex);
        }
    }

    private static String sha256(String text) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(text.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
