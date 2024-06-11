#!/bin/bash

cdo monmean ../inst/extdata/demo_data_2017_month01.nc ../inst/extdata/demo_data_2017_month01_MEAN.nc
cdo monmean ../inst/extdata/demo_data_2017_month02.nc ../inst/extdata/demo_data_2017_month02_MEAN.nc
cdo monmean ../inst/extdata/demo_data_2017_month03.nc ../inst/extdata/demo_data_2017_month03_MEAN.nc
cdo monmean ../inst/extdata/demo_data_2017_month04.nc ../inst/extdata/demo_data_2017_month04_MEAN.nc
cdo monmean ../inst/extdata/demo_data_2017_month05.nc ../inst/extdata/demo_data_2017_month05_MEAN.nc
cdo monmean ../inst/extdata/demo_data_2017_month06.nc ../inst/extdata/demo_data_2017_month06_MEAN.nc
cdo monmean ../inst/extdata/demo_data_2017_month07.nc ../inst/extdata/demo_data_2017_month07_MEAN.nc
cdo monmean ../inst/extdata/demo_data_2017_month08.nc ../inst/extdata/demo_data_2017_month08_MEAN.nc
cdo monmean ../inst/extdata/demo_data_2017_month09.nc ../inst/extdata/demo_data_2017_month09_MEAN.nc
cdo monmean ../inst/extdata/demo_data_2017_month10.nc ../inst/extdata/demo_data_2017_month10_MEAN.nc
cdo monmean ../inst/extdata/demo_data_2017_month11.nc ../inst/extdata/demo_data_2017_month11_MEAN.nc
cdo monmean ../inst/extdata/demo_data_2017_month12.nc ../inst/extdata/demo_data_2017_month12_MEAN.nc

cdo mergetime ../inst/extdata/demo_data_2017_month??_MEAN.nc ../inst/extdata/demo_data_2017_MEAN.nc
