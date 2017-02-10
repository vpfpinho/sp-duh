CREATE TABLE i18n (
    key TEXT,
    pt TEXT,
    en TEXT,
    es TEXT,
    fr TEXT,
    de TEXT,
    it TEXT,
    PRIMARY KEY (key)
);

CREATE INDEX i18n_key_index ON i18n(key);

INSERT INTO i18n(key, en) SELECT CONCAT(lower(key),'_major_singular'), name FROM currency_map;
INSERT INTO i18n(key, en) SELECT CONCAT(lower(key),'_major_plural'), name_plural FROM currency_map;

INSERT INTO i18n (key, pt, en) VALUES ('currency_value_spellout', 
	'{3} {0, plural, =1 {{1}} other {{2}}}{4, plural, =0 {} other { e {7} {4, plural, =1 {{5}} other {{6}}}}}',
	'{3} {0, plural, =1 {{1}} other {{2}}}{4, plural, =0 {} other { and {7} {4, plural, =1 {{5}} other {{6}}}}}'

);
INSERT INTO i18n (key, pt, en) VALUES ('eur_minor_singular', 'cêntimo' , 'cent' );
INSERT INTO i18n (key, pt, en) VALUES ('eur_minor_plural', 'cêntimos', 'cents');

INSERT INTO i18n (key, en) VALUES ('usd_minor_singular', 'cent' );
INSERT INTO i18n (key, en) VALUES ('usd_minor_plural', 'cents');

INSERT INTO i18n (key, en) VALUES ('gbp_minor_singular', 'pence' );
INSERT INTO i18n (key, en) VALUES ('gbp_minor_plural', 'pence');