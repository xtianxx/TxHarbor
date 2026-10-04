--
-- PostgreSQL database dump
--

\restrict c8i35ePsIWFLyZBELSNg45oraf38p1YXTvubbA0c349fGmYPHMSfyg6LazAefJS

-- Dumped from database version 18.6 (Debian 18.6-1.pgdg13+2)
-- Dumped by pg_dump version 18.6 (Debian 18.6-1.pgdg13+2)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: events_015; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.events_015 (
    seq bigint NOT NULL,
    payload bytea
);


ALTER TABLE public.events_015 OWNER TO writer_owner;

--
-- Name: events_015_seq_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

CREATE SEQUENCE public.events_015_seq_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.events_015_seq_seq OWNER TO writer_owner;

--
-- Name: events_015_seq_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: writer_owner
--

ALTER SEQUENCE public.events_015_seq_seq OWNED BY public.events_015.seq;


--
-- Name: snapshot_probe_015; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.snapshot_probe_015 (
    ref text NOT NULL
);


ALTER TABLE public.snapshot_probe_015 OWNER TO writer_owner;

--
-- Name: wallet_sequences_015; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.wallet_sequences_015 (
    id bigint NOT NULL,
    note text
);


ALTER TABLE public.wallet_sequences_015 OWNER TO writer_owner;

--
-- Name: wallet_sequences_015_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

CREATE SEQUENCE public.wallet_sequences_015_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.wallet_sequences_015_id_seq OWNER TO writer_owner;

--
-- Name: wallet_sequences_015_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: writer_owner
--

ALTER SEQUENCE public.wallet_sequences_015_id_seq OWNED BY public.wallet_sequences_015.id;


--
-- Name: events_015 seq; Type: DEFAULT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.events_015 ALTER COLUMN seq SET DEFAULT nextval('public.events_015_seq_seq'::regclass);


--
-- Name: wallet_sequences_015 id; Type: DEFAULT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.wallet_sequences_015 ALTER COLUMN id SET DEFAULT nextval('public.wallet_sequences_015_id_seq'::regclass);


--
-- Data for Name: events_015; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.events_015 (seq, payload) FROM stdin;
\.


--
-- Data for Name: snapshot_probe_015; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.snapshot_probe_015 (ref) FROM stdin;
probe-row-1
\.


--
-- Data for Name: wallet_sequences_015; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.wallet_sequences_015 (id, note) FROM stdin;
\.


--
-- Name: events_015_seq_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.events_015_seq_seq', 1, false);


--
-- Name: wallet_sequences_015_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.wallet_sequences_015_id_seq', 1, false);


--
-- Name: events_015 events_015_seq_key; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.events_015
    ADD CONSTRAINT events_015_seq_key UNIQUE (seq);


--
-- Name: wallet_sequences_015 wallet_sequences_015_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.wallet_sequences_015
    ADD CONSTRAINT wallet_sequences_015_pkey PRIMARY KEY (id);


--
-- Name: TABLE snapshot_probe_015; Type: ACL; Schema: public; Owner: writer_owner
--

GRANT SELECT ON TABLE public.snapshot_probe_015 TO PUBLIC;


--
-- PostgreSQL database dump complete
--

\unrestrict c8i35ePsIWFLyZBELSNg45oraf38p1YXTvubbA0c349fGmYPHMSfyg6LazAefJS

