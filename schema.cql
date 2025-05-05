KEYSPACE: PROJECT

BRONZE TABLE

CREATE TABLE project.bronze_organizations (
    org_index int PRIMARY KEY,
    country text,
    description text,
    founded int,
    industry text,
    name text,
    number_of_employees int,
    organization_id text,
    website text
) 

SILVER TABLE

CREATE TABLE project.silver_organizations (
    org_index int PRIMARY KEY,
    country text,
    description text,
    founded int,
    industry text,
    name text,
    number_of_employees int,
    organization_id text,
    website text
)

GOLD TABLES

GOLD TABLE 1:
CREATE TABLE project.gold_industry_employees (
    industry text PRIMARY KEY,
    average_number_of_employees double
)

GOLD TABLE 2:
CREATE TABLE project.gold_predictions (
    founded int,
    industry text,
    predicted_number_of_employees int,
    PRIMARY KEY (founded, industry)
)

GOLD TABLE 3:
CREATE TABLE project.gold_founded_vs_employees (
    founded_year int PRIMARY KEY,
    number_of_employees int
)