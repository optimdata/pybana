#!/usr/bin/env node

import _ from 'lodash'
import momentTZ from 'moment-timezone'
import moment from 'moment/min/moment-with-locales.min.js'
import numeral from 'numeral'
import {expressionFunction, loader, parse, View} from 'vega'
import {compile} from 'vega-lite'

import 'numeral/locales/fr.js'
import 'numeral/locales/de.js'
import 'numeral/locales/es.js'
import 'numeral/locales/it.js'
import 'numeral/locales/ja.js'
import 'numeral/locales/cs.js'

numeral.register('locale', 'ro', {
  delimiters: {
    thousands: '.',
    decimal: ',',
  },
  abbreviations: {
    thousand: 'k',
    million: 'mil',
    billion: 'mld',
    trillion: 't',
  },
  ordinal: function () {
    return '-'
  },
  currency: {
    symbol: 'RON',
  },
})

numeral.register('format', 'duration', {
  regexps: {
    format: /(!)/,
  },
  format: value => moment.duration(value, 'seconds').humanize(),
  unformat: () => 0,
})

let data = ''
process.stdin.setEncoding('utf8')

process.stdin.on('readable', () => {
  let chunk
  while ((chunk = process.stdin.read()) !== null) {
    data += chunk
  }
})

expressionFunction('momentFormat', (date, fmt) =>
  momentTZ(date).format(fmt),
)
expressionFunction('numeralFormat', (number, fmt) =>
  numeral(number).format(fmt),
)
expressionFunction('inusetimezoneoffset', date =>
  momentTZ.defaultZone ? momentTZ.defaultZone.utcOffset(date) : 0,
)
expressionFunction('kibanaSetTimeFilter', () => null)


process.stdin.on('end', () => {
  data = JSON.parse(_.trim(data))
  if (data.language) {
    numeral.locale(data.language)
    moment.locale(data.language)
    momentTZ.defineLocale(data.language, moment.localeData()._config)
    momentTZ.locale(data.language)
  }
  if (data.timezone) {
    momentTZ.tz.setDefault(data.timezone)
  }
  var spec = data.spec
  if (spec.$schema.includes('vega-lite')) {
    spec = compile(spec).spec
  }
  var view = new View(parse(spec), {
    renderer: 'none',
  }).finalize()
  view
    .toSVG()
    .then(function (svg) {
      process.stdout.write(svg)
    })
    .catch(function (err) {
      process.stderr.write(err.message)
    })
})
