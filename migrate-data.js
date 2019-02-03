const mongoDb = require('mongodb')
const fs = require('fs')
const path = require('path')
const async = require('async')

const customers = require('./m3-customer-data.json')
const customerAddresses = require('./m3-customer-address-data.json')

const url = 'mongodb://localhost:27017/customers-db'

let tasks = []
const limit = parseInt(process.argv[2],10) || customers.length
console.log('limit =' + limit)
var queries = customers.length / limit

mongoDb.MongoClient.connect(url,(err,client)=>{
    if(err){
        console.error(err)
        return process.exit(1)
    }
    const db = client.db('customers-db')

    customers.forEach((customer,index)=> {
        console.log('customer = ' +customer.id+ ' , index = '+index +' , customerAddress = '+ customerAddresses[index].country)
        customer = Object.assign(customer, customerAddresses[index])
        console.log('new cust = ' + customer.id + 'add = '+ customer.country)

        if(index % limit == 0){
            const start = index
            const end = (start + limit >= customers.length)? customers.length-1 : start + limit
            tasks.push((callback)=>{
                console.log('start = ' + start+' , end = ' + end +', customers.length = '+ customers.length)
                console.log('to be inserted = '+ customers[start].id +' to ' + customers[end].id)
                db.collection('customer-details').insertMany(customers.slice(start,end), (err,result)=>{
                    console.log('callback called')
                    callback(err,result)
                })
            })
        }
    } )


    console.log('executing '+tasks.length +' parallel tasks')
    async.parallel(tasks, (error,result)=>{
        if(error){
            console.error(error)
        }
        console.log('Completed')
        //console.log(result)
        client.close()
    })
})
