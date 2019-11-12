<template>
    <Card shadow>
        <p slot="title">Use a card with a shadow effect</p>
        <p>Content of card</p>
        <p>Content of card</p>
        <p>Content of card</p>
    </Card>
</template>

<script>
    export default {
        name: "bContent",
        props:{
            articleID:{
                type: String,
                required: true
            }
        }
    }
</script>

<style scoped>

</style>
