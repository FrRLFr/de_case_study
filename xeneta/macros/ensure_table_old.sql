{% macro ensure_table_old() %}

    {% set query_1 %}
        create table raw.raw_datapoints as (
            select * 
	    from read_csv('../input_files/DE_casestudy_datapoints_*.csv')
        )
    {% endset %}

    {% do run_query(query_1) %}

    {% set query_2 %}
        create table raw.raw_charges as (
            select * 
	        from read_csv(['../input_files/DE_casestudy_charges_1.csv',
                '../input_files/DE_casestudy_charges_2.csv'])
        )
    {% endset %}

    {% do run_query(query_2) %}
    

{% endmacro %}
