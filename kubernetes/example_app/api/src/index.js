"use strict";

const fs = require('fs')

const FILEPATH = '/data/file.txt'

const getValue = () => fs.readFileSync(FILEPATH, 'utf8')
const setValue = (val) => fs.writeFileSync(FILEPATH, val, 'utf8')

module.exports = {
  getValue,
  setValue
}