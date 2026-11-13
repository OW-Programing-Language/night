package org.owlang.night.mail;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class NightMailTest {
    @Test void rendersPlainTextWithNamedVariablesAndBoundaries() {
        assertEquals("Hello Ada! Your code is 1432.", NightMail.template(
                "Hello {{NAME}}! Your code is {{CODE}}.",
                List.of("NAME", "CODE"), List.of("Ada", "1432")));
        assertThrows(IllegalArgumentException.class,
                () -> NightMail.template("Hello {{NAME}}", List.of(), List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> NightMail.template("Hello {{NAME}}", List.of("OTHER"), List.of("Ada")));
        assertThrows(IllegalArgumentException.class,
                () -> NightMail.template("Hello", List.of("X"), List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> NightMail.template("Hello", List.of("X}"), List.of("something")));
    }

    @Test void rejectsEmailHeaderInjectionAndInvalidAddresses() {
        assertEquals("alice@example.com", NightMail.address("alice@example.com"));
        assertThrows(IllegalArgumentException.class,
                () -> NightMail.address("alice@example.com\nBcc: attacker@example.org"));
        assertThrows(IllegalArgumentException.class,
                () -> NightMail.address("Alice <alice@example.com>"));
        assertThrows(IllegalArgumentException.class,
                () -> NightMail.address("not-an-address"));
    }
}
