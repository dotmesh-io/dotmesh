"use strict";

const fs = require('fs')
const tape = require('tape')

const storage = require('../src')

const TEST_VALUE = 'apples'

tape('test storage', t => {

  storage.setValue('oranges')

  const readValue = storage.getValue()

  t.equal(TEST_VALUE, readValue, 'the value is correct')

  t.end()
})