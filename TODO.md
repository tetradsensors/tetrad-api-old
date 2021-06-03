- change lat_lo, lat_hi, etc to camelCase
- nothing in place to convert lat/lon to "region" such as slc, kc, chatt, etc...
    - added `utils.getRegionContainingPoint`
- length scales make us leave elevation region
- in `interpolateQueryLocations` np.arange() doesn't include last point -- switch to linspace

