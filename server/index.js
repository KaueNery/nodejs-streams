import { createServer } from 'node:http';
import { createReadStream } from 'node:fs';
import { Readable, Transform} from 'node:stream';
import { WritableStream , TransformStream} from 'node:stream/web';
import  csvtojson  from 'csvtojson';
import { setTimeout } from 'node:timers/promises';

const PORT = 3000
// curl -i -X OPTIONS -N localhost:3000
createServer(async (request,response) => {
    const headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': '*',
    }
    if(request.method === 'OPTIONS'){
        response.writeHead(204,headers)
        response.end()
        return;
    }

    request.once('close', _ => console.log('connection was closed', items))
    let items = 0;
    Readable.toWeb(createReadStream('./animeflv.csv'))
    .pipeThrough(Transform.toWeb(csvtojson()))
    .pipeThrough(new TransformStream({
        transform(chunk, controller) {
            const data = JSON.parse(Buffer.from(chunk))
            controller.enqueue(JSON.stringify({
                title: data.title,
                description: data.description,
                url_anime: data.url_anime
            }).concat('\n')
            )
        }
    }))
    .pipeTo(new WritableStream({   
        async write(chunk){
            await setTimeout(200)
            items++
            response.write(chunk)
        },
        close(){
            response.end()
        }
    }))

    response.writeHead(200,headers);
})
.listen(PORT)
.on('listening', _ => console.log(`server is running at ${ PORT}`))