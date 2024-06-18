
        create table raw.raw_datapoints as (
            select * 
	    from read_csv('../input_files/DE_casestudy_datapoints_*.csv')
        );

        create table raw.raw_charges as (
            select * 
	        from read_csv(['../input_files/DE_casestudy_charges_1.csv',
                '../input_files/DE_casestudy_charges_2.csv'])
        )
