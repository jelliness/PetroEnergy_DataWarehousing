PGDMP                      }            Petroenergy_Data_Warehousing    16.8    16.8     J           0    0    ENCODING    ENCODING        SET client_encoding = 'UTF8';
                      false            K           0    0 
   STDSTRINGS 
   STDSTRINGS     (   SET standard_conforming_strings = 'on';
                      false            L           0    0 
   SEARCHPATH 
   SEARCHPATH     8   SELECT pg_catalog.set_config('search_path', '', false);
                      false            M           1262    16398    Petroenergy_Data_Warehousing    DATABASE     �   CREATE DATABASE "Petroenergy_Data_Warehousing" WITH TEMPLATE = template0 ENCODING = 'UTF8' LOCALE_PROVIDER = libc LOCALE = 'English_Philippines.1252';
 .   DROP DATABASE "Petroenergy_Data_Warehousing";
                postgres    false            �            1259    17073    account    TABLE     �  CREATE TABLE bronze.account (
    account_id character varying(20),
    email character varying(254),
    account_role character varying(3),
    power_plant_id character varying(10),
    company_id character varying(10),
    account_status character varying(10),
    date_created timestamp without time zone,
    date_updated timestamp without time zone,
    color_code character varying(7)
);
    DROP TABLE bronze.account;
       bronze         heap    postgres    false            G          0    17073    account 
   TABLE DATA           �   COPY bronze.account (account_id, email, account_role, power_plant_id, company_id, account_status, date_created, date_updated, color_code) FROM stdin;
    bronze          postgres    false    228   �       G      x������ � �     